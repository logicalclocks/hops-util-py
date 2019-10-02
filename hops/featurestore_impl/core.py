"""
Featurestore Core Implementation

Module hierarchy of featurestore implementation:

- featurestore
       |
       --- core
             |
             ----dao
             ----exceptions
             ----query_planner
             ----rest
             ----util
             ----featureframes
             ----visualizations
"""

import json

from hops import constants, util, hdfs, tls
from hops.featurestore_impl.dao.common.featurestore_metadata import FeaturestoreMetadata
from hops.featurestore_impl.dao.datasets.training_dataset import TrainingDataset
from hops.featurestore_impl.dao.featuregroups.featuregroup import Featuregroup
from hops.featurestore_impl.dao.stats.statistics import Statistics
from hops.featurestore_impl.dao.storageconnectors.jdbc_connector import JDBCStorageConnector
from hops.featurestore_impl.exceptions.exceptions import FeaturegroupNotFound, HiveDatabaseNotFound, \
    TrainingDatasetNotFound, CouldNotConvertDataframe, TFRecordSchemaNotFound, FeatureDistributionsNotComputed, \
    FeatureCorrelationsNotComputed, FeatureClustersNotComputed, DescriptiveStatisticsNotComputed, HiveNotEnabled, \
    StorageConnectorNotFound, CannotInsertIntoOnDemandFeatureGroup, CannotUpdateStatisticsOfOnDemandFeatureGroup, \
    CannotGetPartitionsOfOnDemandFeatureGroup, StorageConnectorTypeNotSupportedForFeatureImport, \
    CannotDisableOnlineFeatureServingForOnDemandFeatureGroup, CannotEnableOnlineFeatureServingForOnDemandFeatureGroup, \
    OnlineFeaturestoreNotEnabled
from hops.featurestore_impl.featureframes.FeatureFrame import FeatureFrame
from hops.featurestore_impl.query_planner import query_planner
from hops.featurestore_impl.query_planner.f_query import FeatureQuery, FeaturesQuery
from hops.featurestore_impl.query_planner.fg_query import FeaturegroupQuery
from hops.featurestore_impl.query_planner.logical_query_plan import LogicalQueryPlan
from hops.featurestore_impl.rest import rest_rpc
from hops.featurestore_impl.util import fs_utils
from hops.featurestore_impl.visualizations import statistics_plots
from hops.featurestore_impl.online_featurestore import online_featurestore

# for backwards compatibility
try:
    import h5py
except:
    pass

# in case importing in %%local
try:
    from pyspark.sql import SQLContext
    from pyspark.sql.utils import AnalysisException
    from py4j.java_gateway import java_import
except:
    pass

metadata_cache = None


def _get_featurestore_id(featurestore):
    """
    Gets the id of a featurestore (temporary workaround until HOPSWORKS-860 where we use Name to refer to resources)

    Args:
        :featurestore: the featurestore to get the id for

    Returns:
        the id of the feature store

    """
    if metadata_cache is None or featurestore != metadata_cache.featurestore:
        _get_featurestore_metadata(featurestore, update_cache=True)
    return metadata_cache.featurestore.id


def _use_featurestore(spark, featurestore=None):
    """
    Selects the featurestore database in Spark

    Args:
        :spark: the spark session
        :featurestore: the name of the database, defaults to the project's featurestore

    Returns:
        None

    Raises:
        :HiveDatabaseNotFound: when no hive database with the provided featurestore name exist
    """
    if featurestore is None:
        featurestore = fs_utils._do_get_project_featurestore()
    try:
        sql_str = "use " + featurestore
        _run_and_log_sql(spark, sql_str)
    except AnalysisException:
        raise HiveDatabaseNotFound((
            "A hive database for the featurestore {} was not found, have you enabled the "
            "featurestore service in your project?".format(
                featurestore)))


def _get_featurestore_metadata(featurestore=None, update_cache=False):
    """
    Makes a REST call to the appservice in hopsworks to get all metadata of a featurestore (featuregroups and
    training datasets) for the provided featurestore.
    Args:
        :featurestore: the name of the database, defaults to the project's featurestore
        :update_cache: if true the cache is updated
    Returns:
        feature store metadata object
    """
    if featurestore is None:
        featurestore = fs_utils._do_get_project_featurestore()
    global metadata_cache
    if metadata_cache is None or update_cache:
        response_object = rest_rpc._get_featurestore_metadata(featurestore)
        metadata_cache = FeaturestoreMetadata(response_object)
    return metadata_cache


def _convert_field_to_feature_json(field_dict, primary_key, partition_by, online=False, online_types = None):
    """
    Helper function that converts a field in a spark dataframe to a feature dict that is compatible with the
     featurestore API

    Args:
        :field_dict: the dict of spark field to convert
        :primary_key: name of the primary key feature
        :partition_by: a list of columns to partition_by, defaults to the empty list
        :online: boolean flag whether the feature is to be used for online serving
        :online_types: dict with feature name --> online_type. If the field name is present in this dict,
                            the type will be taken from the dict rather than inferred from the spark-dataframe-type

    Returns:
        a feature dict that is compatible with the featurestore API

    """
    f_name = field_dict[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELD_NAME]
    f_type = fs_utils._convert_spark_dtype_to_hive_dtype(field_dict[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELD_TYPE])
    if online:
        if online_types is not None and f_name in online_types:
            f_type_online = online_types[f_name]
        else:
            f_type_online = \
                fs_utils._convert_spark_dtype_to_mysql_dtype(field_dict[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELD_TYPE])
    else:
        f_type_online = None
    f_desc = ""
    if (f_name == primary_key):
        f_primary = True
    else:
        f_primary = False
    if constants.REST_CONFIG.JSON_FEATURE_DESCRIPTION in field_dict[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELD_METADATA]:
        f_desc = field_dict[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELD_METADATA][
            constants.REST_CONFIG.JSON_FEATURE_DESCRIPTION]
    if f_desc == "":
        f_desc = "-"  # comment must be non-empty
    f_partition = f_name in partition_by
    return {
        constants.REST_CONFIG.JSON_FEATURE_NAME: f_name,
        constants.REST_CONFIG.JSON_FEATURE_TYPE: f_type,
        constants.REST_CONFIG.JSON_FEATURE_DESCRIPTION: f_desc,
        constants.REST_CONFIG.JSON_FEATURE_PRIMARY: f_primary,
        constants.REST_CONFIG.JSON_FEATURE_PARTITION: f_partition,
        constants.REST_CONFIG.JSON_FEATURE_ONLINE_TYPE: f_type_online
    }


def _parse_spark_features_schema(spark_schema, primary_key, partition_by=[], online=False, online_types = None):
    """
    Helper function for parsing the schema of a spark dataframe into a list of feature-dicts

    Args:
        :spark_schema: the spark schema to parse
        :primary_key: the column in the dataframe that should be the primary key
        :partition_by: a list of columns to partition_by, defaults to the empty list
        :online: whether the features are to be used for online serving
        :online_types: a dict with feature_name --> online_type, if a feature is present in this dict,
                            the online_type will be taken from the dict rather than inferred from the spark dataframe.

    Returns:
        A list of the parsed features

    """
    raw_schema = json.loads(spark_schema.json())
    raw_fields = raw_schema[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELDS]
    parsed_features = list(map(lambda field: _convert_field_to_feature_json(field, primary_key, partition_by,
                                                                            online=online,
                                                                            online_types=online_types),
                               raw_fields))
    return parsed_features


def _compute_dataframe_stats(spark_df, name, version=1, descriptive_statistics=True,
                             feature_correlation=True, feature_histograms=True, cluster_analysis=True,
                             stat_columns=None, num_bins=20, num_clusters=5,
                             corr_method='pearson'):
    """
    Helper function that computes statistics of a featuregroup or training dataset using spark

    Args:
        :name: the featuregroup or training dataset to update statistics for
        :spark_df: If a spark df is provided it will be used to compute statistics, otherwise the dataframe of the
                   featuregroup will be fetched dynamically from the featurestore
        :version: the version of the featuregroup/training dataset (defaults to 1)
        :descriptive_statistics: a boolean flag whether to compute descriptive statistics (min,max,mean etc)
                                  for the featuregroup/training dataset
        :feature_correlation: a boolean flag whether to compute a feature correlation matrix for the numeric columns
                              in the featuregroup/training dataset
        :feature_histograms: a boolean flag whether to compute histograms for the numeric columns in the
                            featuregroup/training dataset
        :cluster_analysis: a boolean flag whether to compute cluster analysis for the numeric columns in the
                           featuregroup/training dataset
        :stat_columns: a list of columns to compute statistics for (defaults to all columns that are numeric)
        :num_bins: number of bins to use for computing histograms
        :num_clusters: the number of clusters to use for cluster analysis (k-means)
        :corr_method: the method to compute feature correlation with (pearson or spearman)

    Returns:
        feature_corr_data, desc_stats_data, features_histograms_data, cluster_analysis

    """
    if not stat_columns is None:
        spark_df = spark_df.select(stat_columns)
    feature_corr_data = None
    desc_stats_data = None
    features_histograms_data = None
    cluster_analysis_data = None
    spark = util._find_spark()
    _verify_hive_enabled(spark)

    if spark_df.rdd.isEmpty():
        fs_utils._log("Cannot compute statistics on an empty dataframe, the provided dataframe is empty")

    if descriptive_statistics:
        try:
            fs_utils._log("computing descriptive statistics for : {}, version: {}".format(name, version))
            spark.sparkContext.setJobGroup("Descriptive Statistics Computation",
                                           "Analyzing Dataframe Statistics for : {}, version: {}".format(name, version))
            desc_stats_json = fs_utils._compute_descriptive_statistics(spark_df)
            desc_stats_data = fs_utils._structure_descriptive_stats_json(desc_stats_json)
            spark.sparkContext.setJobGroup("", "")
        except Exception as e:
            fs_utils._log(
                "Could not compute descriptive statistics for: {}, version: {}, set the optional argument "
                "descriptive_statistics=False to skip this step,\n error: {}".format(
                    name, version, str(e)))
            desc_stats_data = None

    if feature_correlation:
        try:
            fs_utils._log("computing feature correlation for: {}, version: {}".format(name, version))
            spark.sparkContext.setJobGroup("Feature Correlation Computation",
                                           "Analyzing Feature Correlations for: {}, version: {}".format(name, version))
            spark_df_filtered = fs_utils._filter_spark_df_numeric(spark_df)
            pd_corr_matrix = fs_utils._compute_corr_matrix(spark_df_filtered, corr_method=corr_method)
            feature_corr_data = fs_utils._structure_feature_corr_json(pd_corr_matrix.to_dict())
            spark.sparkContext.setJobGroup("", "")
        except Exception as e:
            fs_utils._log(
                "Could not compute feature correlation for: {}, version: {}, set the optional argument "
                "feature_correlation=False to skip this step,\n error: {}".format(
                    name, version, str(e)))
            feature_corr_data = None

    if feature_histograms:
        try:
            fs_utils._log("computing feature histograms for: {}, version: {}".format(name, version))
            spark.sparkContext.setJobGroup("Feature Histogram Computation",
                                           "Analyzing Feature Distributions for: {}, version: {}".format(name, version))
            spark_df_filtered = fs_utils._filter_spark_df_numeric(spark_df)
            features_histogram_list = fs_utils._compute_feature_histograms(spark_df_filtered, num_bins)
            features_histograms_data = fs_utils._structure_feature_histograms_json(features_histogram_list)
            spark.sparkContext.setJobGroup("", "")
        except Exception as e:
            fs_utils._log(
                "Could not compute feature histograms for: {}, version: {},  "
                "set the optional argument feature_histograms=False "
                "to skip this step,\n error: {}".format(
                    name, version, str(e)))
            features_histograms_data = None

    if cluster_analysis:
        try:
            fs_utils._log("computing cluster analysis for: {}, version: {}".format(name, version))
            spark.sparkContext.setJobGroup("Feature Cluster Analysis",
                                           "Analyzing Feature Clusters for: {}, version: {}".format(name, version))
            spark_df_filtered = fs_utils._filter_spark_df_numeric(spark_df)
            cluster_analysis_raw = fs_utils._compute_cluster_analysis(spark_df_filtered, num_clusters)
            cluster_analysis_data = fs_utils._structure_cluster_analysis_json(cluster_analysis_raw)
            spark.sparkContext.setJobGroup("", "")
        except Exception as e:
            fs_utils._log(
                "Could not compute cluster analysis for: {}, version: {}, "
                "set the optional argument cluster_analysis=False "
                "to skip this step,\n error: {}".format(
                    name, version, str(e)))
            cluster_analysis_data = None

    return feature_corr_data, desc_stats_data, features_histograms_data, cluster_analysis_data


def _get_featuregroup_id(featurestore, featuregroup_name, featuregroup_version):
    """
    Gets the id of a featuregroup (temporary workaround until HOPSWORKS-860 where we use Name to refer to resources)

    Args:
        :featurestore: the featurestore where the featuregroup belongs
        :featuregroup: the featuregroup to get the id for
        :featuregroup_version: the version of the featuregroup

    Returns:
        the id of the featuregroup

    Raises:
        :FeaturegroupNotFound: when the requested featuregroup could not be found in the metadata
    """
    metadata = _get_featurestore_metadata(featurestore, update_cache=False)
    if metadata is None or featurestore != metadata.featurestore:
        metadata = _get_featurestore_metadata(featurestore, update_cache=True)
    for fg in metadata.featuregroups.values():
        if fg.name == featuregroup_name \
                and fg.version == featuregroup_version:
            return fg.id
    raise FeaturegroupNotFound("The featuregroup {} with version: {} "
                               "was not found in the feature store {}".format(featuregroup_name, featuregroup_version,
                                                                              featurestore))


def _do_get_storage_connector(storage_connector_name, featurestore):
    """
    Looks up the metadata of a storage connector given a name

    Args:
        :storage_connector_name: the storage connector name
        :featurestore: the featurestore to query

    Returns:
        the id of the featuregroup

    Raises:
        :FeaturegroupNotFound: when the requested featuregroup could not be found in the metadata
    """
    metadata = _get_featurestore_metadata(featurestore, update_cache=False)
    if metadata is None or featurestore != metadata.featurestore:
        metadata = _get_featurestore_metadata(featurestore, update_cache=True)
    try:
        return metadata.storage_connectors[storage_connector_name]
    except:
        try:
            # Retry with updated metadata
            metadata = _get_featurestore_metadata(featurestore, update_cache=True)
        except KeyError:
            storage_connector_names = list(map(lambda sc: sc.name, metadata.storage_connectors))
            raise StorageConnectorNotFound("Could not find the requested storage connector with name: {} " \
                                           ", among the list of available storage connectors: {}".format(
                storage_connector_name,
                storage_connector_names))


def _do_get_feature(feature, featurestore_metadata, featurestore=None, featuregroup=None, featuregroup_version=1,
                    dataframe_type="spark", jdbc_args = {}, online=False):
    """
    Gets a particular feature (column) from a featurestore, if no featuregroup is specified it queries
    hopsworks metastore to see if the feature exists in any of the featuregroups in the featurestore.
    If the user knows which featuregroup contain the feature, it should be specified as it will improve performance
    of the query.

    Args:
        :feature: the feature name to get
        :featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        :featuregroup: (Optional) the featuregroup where the feature resides
        :featuregroup_version: (Optional) the version of the featuregroup
        :dataframe_type: the type of the returned dataframe (spark, pandas, python or numpy)
        :featurestore_metadata: the metadata of the featurestore to query
        :jdbc_args: jdbc arguments for fetching on-demand feature groups (optional)
        :online: a boolean flag whether to fetch the online feature or the offline one (assuming that the
                 feature group that the feature is stored in has online serving enabled)
                 (for cached feature groups only)

    Returns:
        A spark dataframe with the feature

    """
    if online and ((not featurestore_metadata.settings.online_enabled) or
                       (not featurestore_metadata.featurestore.online_enabled)):
        raise OnlineFeaturestoreNotEnabled("Online Feature Store is not enabled for this project or cluster, "
                                           "talk to an administrator to enable it")
    spark = util._find_spark()
    _verify_hive_enabled(spark)
    _use_featurestore(spark, featurestore)
    spark.sparkContext.setJobGroup("Fetching Feature",
                                   "Getting feature: {} from the featurestore {}".format(feature, featurestore))

    feature_query = FeatureQuery(feature, featurestore_metadata, featurestore, featuregroup, featuregroup_version)
    logical_query_plan = LogicalQueryPlan(feature_query)
    logical_query_plan.create_logical_plan()
    on_demand_featuregroups = list(filter(
        lambda fg: fg.featuregroup_type == featurestore_metadata.settings.on_demand_featuregroup_type,
        logical_query_plan.featuregroups))
    _register_on_demand_featuregroups_as_temp_tables(on_demand_featuregroups, featurestore, jdbc_args)
    logical_query_plan.construct_sql()
    result = _run_and_log_sql(spark, logical_query_plan.sql_str, online=online, featurestore=featurestore)
    spark.sparkContext.setJobGroup("", "")
    return fs_utils._return_dataframe_type(result, dataframe_type)


def _run_and_log_sql(spark, sql_str, online=False, featurestore=None):
    """
    Runs and logs an SQL query with sparkSQL

    Args:
        :spark: the spark session
        :sql_str: the query to run
        :online: if true, run the query using online feature store JDBC connector
        :featurestore: name of the featurestore

    Returns:
        the result of the SQL query
    """
    if not online:
        fs_utils._log("Running sql: {} against offline feature store".format(sql_str))
        return spark.sql(sql_str)
    else:
        fs_utils._log("Running sql: {} against online feature store".format(sql_str))
        featurestore_metadata = _get_featurestore_metadata(featurestore, update_cache=False)
        if online and ((not featurestore_metadata.settings.online_enabled) or
                           (not featurestore_metadata.featurestore.online_enabled)):
            raise OnlineFeaturestoreNotEnabled("Online Feature Store is not enabled for this project or cluster, "
                                               "talk to an administrator to enable it")
        storage_connector = _do_get_online_featurestore_connector(featurestore, featurestore_metadata)
        return online_featurestore._read_jdbc_dataframe(spark, storage_connector, "({}) tmp".format(sql_str))


def _write_featuregroup_hive(spark_df, featuregroup, featurestore, featuregroup_version, mode):
    """
    Writes the contents of a spark dataframe to a feature group Hive table
    Args:
        :spark_df: the data to write
        :featuregroup: the featuregroup to write to
        :featurestore: the featurestore where the featuregroup resides
        :featuregroup_version: the version of the featuregroup
        :mode: the write mode (append or overwrite)

    Returns:
        None

    Raises:
        :ValueError: when the provided write mode does not match the supported write modes (append and overwrite)
    """
    spark = util._find_spark()
    _verify_hive_enabled(spark)
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    spark.sparkContext.setJobGroup("Inserting dataframe into featuregroup",
                                   "Inserting into featuregroup: {} in the featurestore {}".format(featuregroup,
                                                                                                   featurestore))
    _use_featurestore(spark, featurestore)
    tbl_name = fs_utils._get_table_name(featuregroup, featuregroup_version)

    if mode == constants.FEATURE_STORE.FEATURE_GROUP_INSERT_OVERWRITE_MODE:
        _delete_table_contents(featurestore, featuregroup, featuregroup_version)

    if not mode == constants.FEATURE_STORE.FEATURE_GROUP_INSERT_APPEND_MODE and not mode == \
                                                                                    constants.FEATURE_STORE.FEATURE_GROUP_INSERT_OVERWRITE_MODE:
        raise ValueError(
            "The provided write mode {} does not match "
            "the supported modes: ['{}', '{}']".format(mode,
                                                       constants.FEATURE_STORE.FEATURE_GROUP_INSERT_APPEND_MODE,
                                                       constants.FEATURE_STORE.FEATURE_GROUP_INSERT_OVERWRITE_MODE))
    # overwrite is not supported because it will drop the table and create a new one,
    # this means that all the featuregroup metadata will be dropped due to ON DELETE CASCADE
    # to simulate "overwrite" we call hopsworks REST API to drop featuregroup and re-create with the same metadata
    mode = constants.FEATURE_STORE.FEATURE_GROUP_INSERT_APPEND_MODE
    # Specify format hive as it is managed table
    format = "hive"
    # Insert into featuregroup (hive table) with dynamic partitions
    spark_df.write.format(format).mode(mode).insertInto(tbl_name)
    spark.sparkContext.setJobGroup("", "")


def _do_insert_into_featuregroup(df, featuregroup_name, featurestore_metadata, featurestore=None,
                                 featuregroup_version=1, mode="append",
                                 descriptive_statistics=True, feature_correlation=True, feature_histograms=True,
                                 cluster_analysis=True, stat_columns=None, num_bins=20, corr_method='pearson',
                                 num_clusters=5, online=False, offline=True):
    """
    Saves the given dataframe to the specified featuregroup. Defaults to the project-featurestore
    This will append to  the featuregroup. To overwrite a featuregroup, create a new version of the featuregroup
    from the UI and append to that table.

    Args:
        :df: the dataframe containing the data to insert into the featuregroup
        :featuregroup_name: the name of the featuregroup (hive table name)
        :featurestore_metadata: metadata of the feature store
        :featurestore: the featurestore to save the featuregroup to (hive database)
        :featuregroup_version: the version of the featuregroup (defaults to 1)
        :mode: the write mode, only 'overwrite' and 'append' are supported
        :descriptive_statistics: a boolean flag whether to compute descriptive statistics (min,max,mean etc)
                                for the featuregroup
        :feature_correlation: a boolean flag whether to compute a feature correlation matrix for the numeric columns
                              in the featuregroup
        :feature_histograms: a boolean flag whether to compute histograms for the numeric columns in the featuregroup
        :cluster_analysis: a boolean flag whether to compute cluster analysis for the numeric columns in the
                          featuregroup
        :stat_columns: a list of columns to compute statistics for (defaults to all columns that are numeric)
        :num_bins: number of bins to use for computing histograms
        :num_clusters: number of clusters to use for cluster analysis
        :corr_method: the method to compute feature correlation with (pearson or spearman)
        :online: boolean flag whether to insert the data in the online version of the featuregroup
                 (assuming the featuregroup already has online feature serving enabled)
        :offline boolean flag whether to insert the data in the offline version of the featuregroup

    Returns:
        None

    Raises:
        :CouldNotConvertDataframe: in case the provided dataframe could not be converted to a spark dataframe
    """
    if online and ((not featurestore_metadata.settings.online_enabled) or
                       (not featurestore_metadata.featurestore.online_enabled)):
        raise OnlineFeaturestoreNotEnabled("Online Feature Store is not enabled for this project or cluster, "
                                           "talk to an administrator to enable it")
    if featurestore is None:
        featurestore = fs_utils._do_get_project_featurestore()

    fg = query_planner._find_featuregroup(featurestore_metadata.featuregroups, featuregroup_name, featuregroup_version)
    if fg.featuregroup_type == featurestore_metadata.settings.on_demand_featuregroup_type:
        raise CannotInsertIntoOnDemandFeatureGroup("The feature group with name: {} , and version: {} "
                                                   "is an on-demand feature group and cannot be inserted into. "
                                                   "Insert operation is only supported for cached feature groups."
                                                   .format(featuregroup_name, featuregroup_version))
    try:
        spark_df = fs_utils._convert_dataframe_to_spark(df)
    except Exception as e:
        raise CouldNotConvertDataframe(
            "Could not convert the provided dataframe to a spark dataframe which is required in order to save it to "
            "the Feature Store, error: {}".format(str(e)))

    _do_update_featuregroup_stats(featuregroup_name, featurestore_metadata, featuregroup_version=featuregroup_version,
                                  featurestore=featurestore,
                                  descriptive_statistics=descriptive_statistics,
                                  feature_correlation=feature_correlation,
                                  feature_histograms=feature_histograms, cluster_analysis=cluster_analysis,
                                  stat_columns=stat_columns, num_bins=num_bins, num_clusters=num_clusters,
                                  corr_method=corr_method)
    if not online and not offline:
        raise ValueError("online=False and offline=False, nothing to insert. "
                         "Please set online=True to insert data in the online feature group (MySQL), "
                         "and set offline=True to insert in the offline feature group (Hive)")
    if offline:
        fs_utils._log("Inserting data into offline feature group {}...".format(featuregroup_name))
        _write_featuregroup_hive(spark_df, featuregroup_name, featurestore, featuregroup_version, mode)
        fs_utils._log("Inserting data into offline feature group {}... [COMPLETE]".format(featuregroup_name))

    if online:
        fs_utils._log("Inserting data into online feature group {}...".format(featuregroup_name))
        if not fg.is_online():
            raise ValueError("Parameter `online` was set to True, but online feature serving is not enabled "
                             "for this featuregroup. Enable online feature serving with "
                             "`enable_featuregroup_online()` function.")
        storage_connector = _do_get_online_featurestore_connector(featurestore, featurestore_metadata)
        tbl_name = fs_utils._get_table_name(featuregroup_name, featuregroup_version)
        online_featurestore._write_jdbc_dataframe(spark_df, storage_connector, tbl_name,
                                                  write_mode=mode)
        fs_utils._log("Inserting data into online feature group {}... [COMPLETE]".format(featuregroup_name))

    fs_utils._log("Insertion into feature group was successful")


def _do_get_features(features, featurestore_metadata, featurestore=None, featuregroups_version_dict={}, join_key=None,
                     dataframe_type="spark", jdbc_args = {}, online=False):
    """
    Gets a list of features (columns) from the featurestore. If no featuregroup is specified it will query hopsworks
    metastore to find where the features are stored.

    Args:
        :features: a list of features to get from the featurestore
        :featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        :featuregroups: (Optional) a dict with (fg --> version) for all the featuregroups where the features resides
        :featuregroup_version: (Optional) the version of the featuregroup
        :join_key: (Optional) column name to join on
        :dataframe_type: the type of the returned dataframe (spark, pandas, python or numpy)
        :featurestore_metadata: the metadata of the featurestore
        :jdbc_args: jdbc arguments for fetching on-demand feature groups (optional)
        :online: a boolean flag whether to fetch the online version of the features (assuming that the
                 feature groups where the features reside have online serving enabled) (for cached feature groups only)

    Returns:
        A spark dataframe with all the features

    """
    if online and ((not featurestore_metadata.settings.online_enabled) or
                       (not featurestore_metadata.featurestore.online_enabled)):
        raise OnlineFeaturestoreNotEnabled("Online Feature Store is not enabled for this project or cluster, "
                                           "talk to an administrator to enable it")
    if featurestore is None:
        featurestore = fs_utils._do_get_project_featurestore()
    spark = util._find_spark()
    _verify_hive_enabled(spark)
    _use_featurestore(spark, featurestore)
    spark.sparkContext.setJobGroup("Fetching Features",
                                   "Getting features: {} from the featurestore {}".format(features, featurestore))

    features_query = FeaturesQuery(features, featurestore_metadata, featurestore, featuregroups_version_dict, join_key)
    logical_query_plan = LogicalQueryPlan(features_query)
    logical_query_plan.create_logical_plan()
    on_demand_featuregroups = list(filter(
        lambda fg: fg.featuregroup_type == featurestore_metadata.settings.on_demand_featuregroup_type,
        logical_query_plan.featuregroups))
    _register_on_demand_featuregroups_as_temp_tables(on_demand_featuregroups, featurestore, jdbc_args)
    logical_query_plan.construct_sql()
    result = _run_and_log_sql(spark, logical_query_plan.sql_str, online=online, featurestore=featurestore)
    spark.sparkContext.setJobGroup("", "")
    return fs_utils._return_dataframe_type(result, dataframe_type)


def _delete_table_contents(featurestore, featuregroup, featuregroup_version):
    """
    Sends a request to clear the contents of a featuregroup by dropping the featuregroup and recreating it with
    the same metadata.

    Args:
        :featurestore: the featurestore where the featuregroup resides
        :featuregroup: the featuregroup to clear
        :featuregroup_version: version of the featuregroup

    Returns:
        The JSON response

    """
    featuregroup_id = _get_featuregroup_id(featurestore, featuregroup, featuregroup_version)
    featurestore_id = _get_featurestore_id(featurestore)
    response_object = rest_rpc._delete_table_contents(featuregroup_id, featurestore_id)
    # update metadata cache since clearing featuregroup will update its id.
    try:
        _get_featurestore_metadata(featurestore, update_cache=True)
    except:
        pass
    return response_object


def _register_on_demand_featuregroups_as_temp_tables(on_demand_featuregroups, featurestore, jdbc_args={}):
    """
    Registers a list of on-demand featuregroups as temp tables in SparkSQL. Fetches the on-demand featuregroups using
    JDBC and the provided SQL string and then registers the resulting spark dataframes as temporary tables with the name
    featuregroupname_featuregroupversion

    Args:
        :on_demand_featuregroups: metadata of the on-demand feature group to register
        :featurestore: the featurestore to query
        :jdbc_args: jdbc arguments for fetching the on-demand feature group

    Returns:
        None
    """
    for fg in on_demand_featuregroups:
        j_args = {}
        if fs_utils._get_table_name(fg.name, fg.version) in jdbc_args:
            j_args = jdbc_args[fs_utils._get_table_name(fg.name, fg.version)]
        spark_df = _do_get_on_demand_featuregroup(fg, featurestore, j_args)
        spark_df.registerTempTable(fs_utils._get_table_name(fg.name, fg.version))
        fs_utils._log("Registered on-demand feature group: {} with version: {} as temporary table: {}"
                      .format(fg.name, fg.version, fs_utils._get_table_name(fg.name, fg.version)))


def _do_get_on_demand_featuregroup(featuregroup, featurestore, jdbc_args = {}):
    """
    Gets an on-demand featuregroup from a featurestore as a spark dataframe. Uses the JDBC connector to connect
    to the storage backend with Spark and then applies the SQL string for the on-demand feature group and return
    the result

    Args:
        :featuregroup: featuregroup metadata
        :featurestore: the featurestore to query
        :jdbc_args: a dict of argument_name -> value with jdbc connection string arguments to be filled in
                    dynamically at runtime

    Returns:
        a spark dataframe with the contents of the feature group

    """
    jdbc_connector = _do_get_storage_connector(featuregroup.on_demand_featuregroup.jdbc_connector_name,
                                               featurestore=featurestore)
    connection_string = jdbc_connector.connection_string
    connection_string_arguments = jdbc_connector.arguments.split(",")

    # Fill in connection string arguments at runtime
    for connection_string_arg in connection_string_arguments:
        if jdbc_args is not None and connection_string_arg in jdbc_args:
            connection_string = connection_string + connection_string_arg + \
                                constants.DELIMITERS.JDBC_CONNECTION_STRING_VALUE_DELIMITER + \
                                jdbc_args[connection_string_arg] + \
                                constants.DELIMITERS.JDBC_CONNECTION_STRING_DELIMITER
        else:
            if connection_string_arg == constants.FEATURE_STORE.JDBC_TRUSTSTORE_ARG:
                truststore = tls.get_trust_store()
                connection_string = connection_string + constants.FEATURE_STORE.JDBC_TRUSTSTORE_ARG + \
                                    constants.DELIMITERS.JDBC_CONNECTION_STRING_VALUE_DELIMITER + truststore + \
                                    constants.DELIMITERS.JDBC_CONNECTION_STRING_DELIMITER
            if connection_string_arg == constants.FEATURE_STORE.JDBC_TRUSTSTORE_PW_ARG:
                pw = tls.get_key_store_pwd()
                connection_string = connection_string + constants.FEATURE_STORE.JDBC_TRUSTSTORE_PW_ARG + \
                                    constants.DELIMITERS.JDBC_CONNECTION_STRING_VALUE_DELIMITER + pw + \
                                    constants.DELIMITERS.JDBC_CONNECTION_STRING_DELIMITER
            if connection_string_arg == constants.FEATURE_STORE.JDBC_KEYSTORE_ARG:
                keystore = tls.get_key_store()
                connection_string = connection_string + constants.FEATURE_STORE.JDBC_KEYSTORE_ARG + \
                                    constants.DELIMITERS.JDBC_CONNECTION_STRING_VALUE_DELIMITER + keystore + \
                                    constants.DELIMITERS.JDBC_CONNECTION_STRING_DELIMITER
            if connection_string_arg == constants.FEATURE_STORE.JDBC_KEYSTORE_PW_ARG:
                pw = tls.get_key_store_pwd()
                connection_string = connection_string + constants.FEATURE_STORE.JDBC_KEYSTORE_PW_ARG + \
                                    constants.DELIMITERS.JDBC_CONNECTION_STRING_VALUE_DELIMITER + pw + \
                                    constants.DELIMITERS.JDBC_CONNECTION_STRING_DELIMITER

    # Add custom JDBC dialects
    spark = util._find_spark()
    gw = spark.sparkContext._gateway
    java_import(gw.jvm, constants.FEATURE_STORE.IMPORT_HOPS_UTIL_FEATURESTORE_HELPER)
    gw.jvm.org.apache.spark.sql.jdbc.JdbcDialects.registerDialect(
        gw.jvm.io.hops.util.featurestore.FeaturestoreHelper.getHiveJdbcDialect())

    # Read using Spark, the JDBC connector, and the SQL query of the on-demand fg
    spark_df = spark.read.format(constants.SPARK_CONFIG.SPARK_JDBC_FORMAT) \
        .option(constants.SPARK_CONFIG.SPARK_JDBC_URL, connection_string) \
        .option(constants.SPARK_CONFIG.SPARK_JDBC_DBTABLE, "(" + featuregroup.on_demand_featuregroup.query + ") fs_q") \
        .load()

    # Remove column prefixes
    column_names = \
        list(map(lambda field: field[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELD_NAME].replace("fs_q.", ""),
                 json.loads(spark_df.schema.json())[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELDS]))
    renamed_spark_df = spark_df.toDF(*column_names)
    spark.sparkContext.setJobGroup("", "")
    return renamed_spark_df


def _do_get_featuregroup(featuregroup_name, featurestore_metadata, featurestore=None,
                         featuregroup_version=1, dataframe_type="spark", jdbc_args = {}, online=False):
    """
    Gets a featuregroup from a featurestore as a spark dataframe

    Args:
        :featuregroup_name: name of the featuregroup to get
        :featurestore_metadata: featurestore metadata
        :featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        :featuregroup_version: (Optional) the version of the featuregroup
        :dataframe_type: the type of the returned dataframe (spark, pandas, python or numpy)
        :jdbc_args: a dict of argument_name -> value with jdbc connection string arguments to be filled in
                    dynamically at runtime for fetching on-demand feature groups
        :online: a boolean flag whether to fetch the online feature group or the offline one (assuming that the
                 feature group has online serving enabled) (for cached feature groups only)

    Returns:
        a spark dataframe with the contents of the featurestore

    """
    if online and ((not featurestore_metadata.settings.online_enabled) or
                       (not featurestore_metadata.featurestore.online_enabled)):
        raise OnlineFeaturestoreNotEnabled("Online Feature Store is not enabled for this project or cluster, "
                                           "talk to an administrator to enable it")
    if featurestore is None:
        featurestore = fs_utils._do_get_project_featurestore()
    fg = query_planner._find_featuregroup(featurestore_metadata.featuregroups, featuregroup_name, featuregroup_version)
    spark = util._find_spark()
    spark.sparkContext.setJobGroup("Fetching Feature group",
                                   "Getting feature group: {} from the featurestore {}".format(featuregroup_name,
                                                                                               featurestore))
    if fg.featuregroup_type == featurestore_metadata.settings.on_demand_featuregroup_type:
        return _do_get_on_demand_featuregroup(fg, featurestore, jdbc_args)
    if fg.featuregroup_type == featurestore_metadata.settings.cached_featuregroup_type:
        return _do_get_cached_featuregroup(featuregroup_name, featurestore, featuregroup_version, dataframe_type,
                                           online=online)

    raise ValueError("The feature group type: "
                     + fg.featuregroup_type + " was not recognized. Recognized types include: {} and {}" \
                     .format(featurestore_metadata.settings.on_demand_featuregroup_type,
                             featurestore_metadata.settings.cached_featuregroup_type))


def _do_get_cached_featuregroup(featuregroup_name, featurestore=None, featuregroup_version=1, dataframe_type="spark",
                                online=False):
    """
    Gets a cached featuregroup from a featurestore as a spark dataframe

    Args:
        :featuregroup_name: name of the featuregroup to get
        :featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        :featuregroup_version: (Optional) the version of the featuregroup
        :dataframe_type: the type of the returned dataframe (spark, pandas, python or numpy)
        :online: a boolean flag whether to fetch the online feature group or the offline one (assuming that the
                 feature group has online serving enabled)

    Returns:
        a spark dataframe with the contents of the feature group

    """
    spark = util._find_spark()
    _verify_hive_enabled(spark)
    _use_featurestore(spark, featurestore)
    featuregroup_query = FeaturegroupQuery(featuregroup_name, featurestore, featuregroup_version)
    logical_query_plan = LogicalQueryPlan(featuregroup_query)
    logical_query_plan.create_logical_plan()
    logical_query_plan.construct_sql()
    result = _run_and_log_sql(spark, logical_query_plan.sql_str, online=online, featurestore=featurestore)
    spark.sparkContext.setJobGroup("", "")
    return fs_utils._return_dataframe_type(result, dataframe_type)


def _do_get_training_dataset(training_dataset_name, featurestore_metadata, training_dataset_version=1,
                             dataframe_type="spark", featurestore=None):
    """
    Reads a training dataset into a spark dataframe

    Args:
        :training_dataset_name: the name of the training dataset to read
        :training_dataset_version: the version of the training dataset
        :dataframe_type: the type of the returned dataframe (spark, pandas, python or numpy)
        :featurestore_metadata: metadata of the featurestore

    Returns:
        A spark dataframe with the given training dataset data
    """
    if featurestore is None:
        featurestore = fs_utils._do_get_project_featurestore()

    training_dataset = query_planner._find_training_dataset(featurestore_metadata.training_datasets,
                                                            training_dataset_name,
                                                            training_dataset_version)
    if training_dataset.training_dataset_type == featurestore_metadata.settings.hopsfs_training_dataset_type:
        path = fs_utils._get_hopsfs_training_dataset_path(training_dataset.name,
                                                          training_dataset.hopsfs_training_dataset.hdfs_store_path,
                                                          training_dataset.data_format)
    else:
        s3_connector = _do_get_storage_connector(training_dataset.external_training_dataset.s3_connector_name,
                                                 featurestore)
        fs_utils._setup_s3_credentials_for_spark(s3_connector.access_key, s3_connector.secret_key, util._find_spark())
        path = fs_utils._get_external_training_dataset_path(training_dataset.name, training_dataset.version,
            s3_connector.bucket)

    featureframe = FeatureFrame.get_featureframe(path=path, dataframe_type=dataframe_type,
                                                 data_format=training_dataset.data_format,
                                                 training_dataset = training_dataset
                                                 )
    return featureframe.read_featureframe(util._find_spark())


def _do_create_training_dataset(df, training_dataset, description="", featurestore=None,
                                data_format="tfrecords", training_dataset_version=1,
                                jobs=[], descriptive_statistics=True, feature_correlation=True,
                                feature_histograms=True, cluster_analysis=True, stat_columns=None, num_bins=20,
                                corr_method='pearson', num_clusters=5, petastorm_args={}, fixed=True,
                                storage_connector=None):
    """
    Creates a new training dataset from a dataframe, saves metadata about the training dataset to the database
    and saves the materialized dataset on hdfs

    Args:
        :df: the dataframe to create the training dataset from
        :training_dataset: the name of the training dataset
        :description: a description of the training dataset
        :featurestore: the featurestore that the training dataset is linked to
        :data_format: the format of the materialized training dataset
        :training_dataset_version: the version of the training dataset (defaults to 1)
        :jobs: list of job names linked to the training dataset
        :descriptive_statistics: a boolean flag whether to compute descriptive statistics (min,max,mean etc)
                                for the featuregroup
        :feature_correlation: a boolean flag whether to compute a feature correlation matrix for the numeric columns
                              in the featuregroup
        :feature_histograms: a boolean flag whether to compute histograms for the numeric columns in the featuregroup
        :cluster_analysis: a boolean flag whether to compute cluster analysis for the numeric columns in the
                           featuregroup
        :stat_columns: a list of columns to compute statistics for (defaults to all columns that are numeric)
        :num_bins: number of bins to use for computing histograms
        :num_clusters: number of clusters to use for cluster analysis
        :corr_method: the method to compute feature correlation with (pearson or spearman)
        :petastorm_args: a dict containing petastorm parameters for serializing a dataset in the
                         petastorm format. Required parameters are: 'schema'
        :fixed: boolean flag indicating whether array columns should be treated with fixed size or variable size
        :storage_connector: the storage connector where the training dataset is stored

    Returns:
        None

    Raises:
        :CouldNotConvertDataframe: in case the provided dataframe could not be converted to a spark dataframe
    """
    if featurestore is None:
        featurestore = fs_utils._do_get_project_featurestore()
    try:
        spark_df = fs_utils._convert_dataframe_to_spark(df)
    except Exception as e:
        raise CouldNotConvertDataframe(
            "Could not convert the provided dataframe to a spark dataframe which is required in order "
            "to save it to the Feature Store, error: {}".format(
                str(e)))

    if storage_connector is None:
        storage_connector = _do_get_storage_connector(fs_utils._do_get_project_training_datasets_sink(), featurestore)
    fs_utils._validate_metadata(training_dataset, spark_df.dtypes, description)
    feature_corr_data, training_dataset_desc_stats_data, features_histogram_data, cluster_analysis_data = \
        _compute_dataframe_stats(
            spark_df, training_dataset, version=training_dataset_version,
            descriptive_statistics=descriptive_statistics, feature_correlation=feature_correlation,
            feature_histograms=feature_histograms, cluster_analysis=cluster_analysis, stat_columns=stat_columns,
            num_bins=num_bins,
            corr_method=corr_method,
            num_clusters=num_clusters)
    features_schema = _parse_spark_features_schema(spark_df.schema, None)
    featurestore_id = _get_featurestore_id(featurestore)
    featurestore_metadata = _get_featurestore_metadata(featurestore, update_cache=False)
    hopsfs_connector_id = None
    s3_connector_id = None
    if storage_connector.type == featurestore_metadata.settings.hopsfs_connector_type:
        external = False
        hopsfs_connector_id = storage_connector.id
    else:
        external = True
        s3_connector_id = storage_connector.id
    training_dataset_type, training_dataset_type_dto = \
        fs_utils._get_training_dataset_type_info(featurestore_metadata, external)
    td_json = rest_rpc._create_training_dataset_rest(
        training_dataset, featurestore_id, description, training_dataset_version,
        data_format, jobs, features_schema, feature_corr_data, training_dataset_desc_stats_data,
        features_histogram_data, cluster_analysis_data, training_dataset_type, training_dataset_type_dto,
        featurestore_metadata.settings, hopsfs_connector_id=hopsfs_connector_id, s3_connector_id=s3_connector_id)
    td = TrainingDataset(td_json)
    if td.training_dataset_type == featurestore_metadata.settings.hopsfs_training_dataset_type:
        path = util.abspath(td.hopsfs_training_dataset.hdfs_store_path)
        if data_format == constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT:
            try:
                tf_record_schema_json = fs_utils._get_dataframe_tf_record_schema_json(spark_df, fixed=fixed)[1]
                fs_utils._store_tf_record_schema_hdfs(tf_record_schema_json, path)
            except Exception as e:
                fs_utils._log("Could not infer tfrecords schema for the dataframe, {}".format(str(e)))

        featureframe = FeatureFrame.get_featureframe(path=path +
                                                          constants.DELIMITERS.SLASH_DELIMITER + td.name,
                                                     data_format=data_format, df=spark_df,
                                                     write_mode=constants.SPARK_CONFIG.SPARK_OVERWRITE_MODE,
                                                     training_dataset=td,
                                                     petastorm_args=petastorm_args)
    else:
        s3_connector = _do_get_storage_connector(td.external_training_dataset.s3_connector_name, featurestore)
        fs_utils._setup_s3_credentials_for_spark(s3_connector.access_key, s3_connector.secret_key, util._find_spark())
        path = fs_utils._get_external_training_dataset_path(td.name, td.version,
                                                            s3_connector.bucket)
        featureframe = FeatureFrame.get_featureframe(path=path,
                                                     data_format=data_format, df=spark_df,
                                                     write_mode=constants.SPARK_CONFIG.SPARK_OVERWRITE_MODE,
                                                     training_dataset=td,
                                                     petastorm_args=petastorm_args)
    spark = util._find_spark()
    spark.sparkContext.setJobGroup("Materializing dataframe as training dataset",
                                   "Saving training dataset in path: {} in format {}".format(path, data_format))
    featureframe.write_featureframe()
    spark.sparkContext.setJobGroup("", "")
    # update metadata cache
    try:
        _get_featurestore_metadata(featurestore, update_cache=True)
    except:
        pass
    fs_utils._log("Training Dataset created successfully")


def _do_update_featuregroup_stats(featuregroup_name, featurestore_metadata, spark_df=None, featuregroup_version=1,
                                  featurestore=None, descriptive_statistics=True,
                                  feature_correlation=True, feature_histograms=True, cluster_analysis=True,
                                  stat_columns=None, num_bins=20, num_clusters=5, corr_method='pearson'):
    """
    Args:
         :featuregroup_name: name of the featuregroup to update the statistics for
         :featurestore_metadata: metadata of the featurestore
         :spark_df: if not None, use this dataframe to compute the statistics, otherwise read the feature group
         :featuregroup_version: the version of the featuregroup (defaults to 1)
         :featurestore: the featurestore where the featuregroup resides (defaults to the project's featurestore)
         :descriptive_statistics: a boolean flag whether to compute descriptive statistics (min,max,mean etc)
                                 for the featuregroup
         :feature_correlation: a boolean flag whether to compute a feature correlation matrix for the numeric columns
                              in the featuregroup
         :feature_histograms: a boolean flag whether to compute histograms for the numeric columns in the featuregroup
         :cluster_analysis: a boolean flag whether to compute cluster analysis for the numeric columns in the
                           featuregroup
         :stat_columns: a list of columns to compute statistics for (defaults to all columns that are numeric)
         :num_bins: number of bins to use for computing histograms
         :num_clusters: the number of clusters to use in clustering analysis (k-means)
         :corr_method: the method to compute feature correlation with (pearson or spearman)

    Returns:
         DTO of the created feature group
    """
    if featurestore is None:
        featurestore = fs_utils._do_get_project_featurestore()

    if spark_df is None:
        spark_df = _do_get_cached_featuregroup(featuregroup_name, featurestore, featuregroup_version,
                                               dataframe_type=constants.FEATURE_STORE.DATAFRAME_TYPE_SPARK)

    fg = query_planner._find_featuregroup(featurestore_metadata.featuregroups, featuregroup_name, featuregroup_version)
    if fg.featuregroup_type == featurestore_metadata.settings.on_demand_featuregroup_type:
        raise CannotUpdateStatisticsOfOnDemandFeatureGroup("The feature group with name: {} , and version: {} "
                                                           "is an on-demand feature group and therefore there are no "
                                                           "statistics stored about it in Hopsworks that can be updated."
                                                           "Update statistics operation is only supported for cached "
                                                           "feature groups."
                                                           .format(featuregroup_name, featuregroup_version))

    feature_corr_data, featuregroup_desc_stats_data, features_histogram_data, cluster_analysis_data = \
        _compute_dataframe_stats(spark_df,
                                 featuregroup_name, version=featuregroup_version,
                                 descriptive_statistics=descriptive_statistics,
                                 feature_correlation=feature_correlation,
                                 feature_histograms=feature_histograms, cluster_analysis=cluster_analysis,
                                 stat_columns=stat_columns,
                                 num_bins=num_bins, corr_method=corr_method,
                                 num_clusters=num_clusters)
    featuregroup_id = fg.id
    featurestore_id = featurestore_metadata.featurestore.id
    featuregroup_type, featuregroup_type_dto = fs_utils._get_cached_featuregroup_type_info(featurestore_metadata)
    jobs = []
    if util.get_job_name() is not None:
        jobs.append(util.get_job_name())
    featuregroup = Featuregroup(rest_rpc._update_featuregroup_stats_rest(
        featuregroup_id, featurestore_id, feature_corr_data, featuregroup_desc_stats_data, features_histogram_data,
        cluster_analysis_data, featuregroup_type, featuregroup_type_dto, jobs))
    return featuregroup


def _do_update_training_dataset_stats(training_dataset_name, featurestore_metadata, spark_df=None, featurestore=None,
                                      training_dataset_version=1, descriptive_statistics=True, feature_correlation=True,
                                      feature_histograms=True, cluster_analysis=True, stat_columns=None, num_bins=20,
                                      corr_method='pearson', num_clusters=5):
    """
    Args:
         :training_dataset_name: the training dataset to update the statistics for
         :featurestore_metadata: metadata of the feature store
         :spark_df: if not None, use this dataframe to compute the statistics, otherwise read the training dataset
         :featurestore: the featurestore where the training dataset resides (defaults to the project's featurestore)
         :training_dataset_version: the version of the training dataset (defaults to 1)
         :descriptive_statistics: a boolean flag whether to compute descriptive statistics (min,max,mean etc) for
                                 the featuregroup
         :feature_correlation: a boolean flag whether to compute a feature correlation matrix for the numeric columns
                              in the featuregroup
         :feature_histograms: a boolean flag whether to compute histograms for the numeric columns in the featuregroup
         :cluster_analysis:   a boolean flag whether to compute cluster analysis for the numeric columns in
                           the featuregroup
         :stat_columns: a list of columns to compute statistics for (defaults to all columns that are numeric)
         :num_bins: number of bins to use for computing histograms
         :corr_method: the method to compute feature correlation with (pearson or spearman)
         :num_clusters: the number of clusters to use in clustering analysis (k-means)

    Returns:
         DTO of the created training dataset
    """
    if spark_df is None:
        spark_df = _do_get_training_dataset(training_dataset_name, featurestore_metadata,
                                            training_dataset_version=training_dataset_version,
                                            dataframe_type=constants.FEATURE_STORE.DATAFRAME_TYPE_SPARK)
    training_dataset = query_planner._find_training_dataset(featurestore_metadata.training_datasets,
                                                            training_dataset_name, training_dataset_version)
    feature_corr_data, training_dataset_desc_stats_data, features_histogram_data, cluster_analysis_data = \
        _compute_dataframe_stats(
            spark_df, training_dataset_name, version=training_dataset_version,
            descriptive_statistics=descriptive_statistics, feature_correlation=feature_correlation,
            feature_histograms=feature_histograms, cluster_analysis=cluster_analysis, stat_columns=stat_columns,
            num_bins=num_bins, corr_method=corr_method,
            num_clusters=num_clusters)
    training_dataset_id = training_dataset.id
    featurestore_id = featurestore_metadata.featurestore.id
    training_dataset_type, training_dataset_type_dto = \
        fs_utils._get_training_dataset_type_info(featurestore_metadata,
                                                 external=(training_dataset.training_dataset_type ==
                                                           featurestore_metadata.settings.external_training_dataset_type))
    jobs = []
    if util.get_job_name() is not None:
        jobs.append(util.get_job_name())
    td = TrainingDataset(rest_rpc._update_training_dataset_stats_rest(
        training_dataset_id, featurestore_id, feature_corr_data, training_dataset_desc_stats_data,
        features_histogram_data, cluster_analysis_data, training_dataset_type, training_dataset_type_dto, jobs))
    return td


def _do_insert_into_training_dataset(
        df, training_dataset_name, featurestore_metadata, featurestore=None, training_dataset_version=1,
        descriptive_statistics=True, feature_correlation=True,
        feature_histograms=True, cluster_analysis=True, stat_columns=None, num_bins=20, corr_method='pearson',
        num_clusters=5, write_mode="overwrite", fixed=True):
    """
    Inserts the data in a training dataset from a spark dataframe (append or overwrite)

    Args:
        :df: the dataframe to write
        :training_dataset_name: the name of the training dataset
        :featurestore: the featurestore that the training dataset is linked to
        :featurestore_metadata: metadata of the featurestore
        :training_dataset_version: the version of the training dataset (defaults to 1)
        :descriptive_statistics: a boolean flag whether to compute descriptive statistics (min,max,mean etc)
                                 for the featuregroup
        :feature_correlation: a boolean flag whether to compute a feature correlation matrix for the numeric columns
                              in the featuregroup
        :feature_histograms: a boolean flag whether to compute histograms for the numeric columns in the featuregroup
        :cluster_analysis: a boolean flag whether to compute cluster analysis for the numeric columns in
                           the featuregroup
        :stat_columns: a list of columns to compute statistics for (defaults to all columns that are numeric)
        :num_bins: number of bins to use for computing histograms
        :num_clusters: number of clusters to use for cluster analysis
        :corr_method: the method to compute feature correlation with (pearson or spearman)
        :write_mode: spark write mode ('append' or 'overwrite'). Note: append is not supported for tfrecords datasets.
        :fixed: boolean flag indicating whether array columns should be treated with fixed size or variable size

    Returns:
        None

    Raises:
        :CouldNotConvertDataframe: in case the provided dataframe could not be converted to a spark dataframe
    """
    print("insert_into_training_dataset")
    try:
        spark_df = fs_utils._convert_dataframe_to_spark(df)
    except Exception as e:
        raise CouldNotConvertDataframe(
            "Could not convert the provided dataframe to a spark dataframe which is required in order to save it to "
            "the Feature Store, error: {}".format(str(e)))

    if featurestore is None:
        featurestore = fs_utils._do_get_project_featurestore()

    td = _do_update_training_dataset_stats(training_dataset_name, featurestore_metadata, spark_df=spark_df,
                                           featurestore=featurestore, training_dataset_version=training_dataset_version,
                                           descriptive_statistics=descriptive_statistics,
                                           feature_correlation=feature_correlation,
                                           feature_histograms=feature_histograms,
                                           cluster_analysis=cluster_analysis, stat_columns=stat_columns,
                                           num_bins=num_bins,
                                           corr_method=corr_method, num_clusters=num_clusters)
    data_format = td.data_format
    if td.training_dataset_type == featurestore_metadata.settings.hopsfs_training_dataset_type:
        path = util.abspath(td.hopsfs_training_dataset.hdfs_store_path)
        if data_format == constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT:
            try:
                tf_record_schema_json = fs_utils._get_dataframe_tf_record_schema_json(spark_df, fixed=fixed)[1]
                fs_utils._store_tf_record_schema_hdfs(tf_record_schema_json, path)
            except Exception as e:
                fs_utils._log("Could not infer tfrecords schema for the dataframe, {}".format(str(e)))

        featureframe = FeatureFrame.get_featureframe(path=path + constants.DELIMITERS.SLASH_DELIMITER + td.name,
                                                     data_format=data_format, df=spark_df,
                                                     write_mode=constants.SPARK_CONFIG.SPARK_OVERWRITE_MODE,
                                                     training_dataset=td)
    else:
        s3_connector = _do_get_storage_connector(td.external_training_dataset.s3_connector_name, featurestore)
        fs_utils._setup_s3_credentials_for_spark(s3_connector.access_key, s3_connector.secret_key, util._find_spark())
        path = fs_utils._get_external_training_dataset_path(td.name, td.version,
                                                            s3_connector.bucket)
        featureframe = FeatureFrame.get_featureframe(path=path,
                                                     data_format=data_format, df=spark_df,
                                                     write_mode=write_mode,
                                                     training_dataset=td)
    spark = util._find_spark()
    _verify_hive_enabled(spark)
    spark.sparkContext.setJobGroup("Materializing dataframe as training dataset",
                                   "Saving training dataset in path: {} in format {}".format(path, data_format))
    print("Writing Feature Frame, data format: {}".format(data_format))
    featureframe.write_featureframe()
    spark.sparkContext.setJobGroup("", "")


def _get_training_dataset_id(featurestore, training_dataset_name, training_dataset_version):
    """
    Gets the id of a training_Dataset (temporary workaround until HOPSWORKS-860 where we use Name to refer to resources)

    Args:
        :featurestore: the featurestore where the featuregroup belongs
        :training_dataset_name: the training_dataset to get the id for
        :training_dataset_version: the id of the training dataset

    Returns:
        the id of the training dataset

    Raises:
        :TrainingDatasetNotFound: if the requested trainining dataset could not be found
    """
    metadata = _get_featurestore_metadata(featurestore, update_cache=False)
    if metadata is None or featurestore != metadata.featurestore.name:
        metadata = _get_featurestore_metadata(featurestore, update_cache=True)
    for td in metadata.training_datasets.values():
        if td.name == training_dataset_name and td.version == training_dataset_version:
            return td.id
    raise TrainingDatasetNotFound("The training dataset {} with version: {} "
                                  "was not found in the feature store {}".format(
        training_dataset_name, training_dataset_version, featurestore))


def _do_get_training_datasets(featurestore_metadata):
    """
    Gets a list of all training datasets in a featurestore

    Args:
        :featurestore_metadata: metadata of the featurestore

    Returns:
        A list of names of the training datasets in this featurestore
    """
    training_dataset_names = list(
        map(lambda td: fs_utils._get_table_name(td.name,
                                                td.version),
            featurestore_metadata.training_datasets.values()))
    return training_dataset_names


def _do_get_storage_connectors(featurestore_metadata):
    """
    Gets a list of all storage connectors and their type in a featurestore

    Args:
        :featurestore_metadata: metadata of the featurestore

    Returns:
        A list of names of the storage connectors in this featurestore and their type
    """
    return list(map(lambda sc: (sc.name, sc.type), featurestore_metadata.storage_connectors.values()))



def _do_get_training_dataset_path(training_dataset_name, featurestore_metadata, training_dataset_version=1):
    """
    Gets the HDFS path to a training dataset with a specific name and version in a featurestore

    Args:
        :training_dataset_name: name of the training dataset
        :featurestore_metadata: metadata of the featurestore
        :training_dataset_version: version of the training dataset

    Returns:
        The HDFS path to the training dataset
    """
    training_dataset = query_planner._find_training_dataset(featurestore_metadata.training_datasets,
                                                            training_dataset_name,
                                                            training_dataset_version)
    hdfs_path = training_dataset.hopsfs_training_dataset.hdfs_store_path + \
                constants.DELIMITERS.SLASH_DELIMITER + training_dataset.name
    data_format = training_dataset.data_format
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_NPY_FORMAT:
        hdfs_path = hdfs_path + constants.FEATURE_STORE.TRAINING_DATASET_NPY_SUFFIX
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_HDF5_FORMAT:
        hdfs_path = hdfs_path + constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_IMAGE_FORMAT:
        hdfs_path = training_dataset.hopsfs_training_dataset.hdfs_store_path
    # abspath means "hdfs://namenode:port/ is preprended
    abspath = util.abspath(hdfs_path)
    return abspath


def _do_get_training_dataset_tf_record_schema(training_dataset_name, featurestore_metadata, training_dataset_version=1,
                                              featurestore=None):
    """
    Gets the tf record schema for a training dataset that is stored in tfrecords format

    Args:
        :training_dataset: the training dataset to get the tfrecords schema for
        :training_dataset_version: the version of the training dataset
        :featurestore_metadata: metadata of the featurestore

    Returns:
        the tf records schema

    Raises:
        :TFRecordSchemaNotFound: if a tfrecord schema for the given training dataset could not be found
    """
    training_dataset = query_planner._find_training_dataset(featurestore_metadata.training_datasets,
                                                            training_dataset_name,
                                                            training_dataset_version)
    if training_dataset.data_format != \
            constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT:
        raise TFRecordSchemaNotFound(
            "Cannot fetch tf records schema for a training dataset that is not stored in tfrecords format, "
            "this training dataset is stored in format: {}".format(
                training_dataset.data_format))
    hdfs_path = util.abspath(training_dataset.hopsfs_training_dataset.hdfs_store_path)
    tf_record_json_schema = json.loads(hdfs.load(
        hdfs_path + constants.DELIMITERS.SLASH_DELIMITER +
        constants.FEATURE_STORE.TRAINING_DATASET_TF_RECORD_SCHEMA_FILE_NAME))
    return fs_utils._convert_tf_record_schema_json_to_dict(tf_record_json_schema)


def _do_get_featuregroup_partitions(featuregroup_name, featurestore_metadata, featurestore=None, featuregroup_version=1,
                                    dataframe_type="spark"):
    """
    Gets the partitions of a featuregroup

     Args:
        :featuregroup_name: the featuregroup to get partitions for
        :featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        :featuregroup_version: the version of the featuregroup, defaults to 1
        :dataframe_type: the type of the returned dataframe (spark, pandas, python or numpy)

     Returns:
        a dataframe with the partitions of the featuregroup
     """
    fg = query_planner._find_featuregroup(featurestore_metadata.featuregroups, featuregroup_name, featuregroup_version)
    if fg.featuregroup_type == featurestore_metadata.settings.on_demand_featuregroup_type:
        raise CannotGetPartitionsOfOnDemandFeatureGroup("The feature group with name: {} , and version: {} "
                                                        "is an on-demand feature group. "
                                                        "Get partitions operation is only supported for "
                                                        "cached feature groups."
                                                        .format(featuregroup_name, featuregroup_version))
    spark = util._find_spark()
    _verify_hive_enabled(spark)
    spark.sparkContext.setJobGroup("Fetching Partitions of a Featuregroup",
                                   "Getting partitions for feature group: {} from the featurestore {}".format(
                                       featuregroup_name, featurestore))
    _use_featurestore(spark, featurestore)
    sql_str = "SHOW PARTITIONS " + fs_utils._get_table_name(featuregroup_name, featuregroup_version)
    result = _run_and_log_sql(spark, sql_str)
    spark.sparkContext.setJobGroup("", "")
    return fs_utils._return_dataframe_type(result, dataframe_type)


def _do_visualize_featuregroup_distributions(featuregroup_name, featurestore=None, featuregroup_version=1,
                                             figsize=(16, 12), color='lightblue', log=False, align="center"):
    """
    Creates a matplotlib figure of the feature distributions in a featuregroup in the featurestore.

    1. Fetches the stored statistics for the featuregroup
    2. If the feature distributions have been computed for the featuregroup, create the figure

    Args:
        :featuregroup_name: the name of the featuregroup
        :featurestore: the featurestore where the featuregroup resides
        :featuregroup_version: the version of the featuregroup
        :figsize: size of the figure
        :color: the color of the histograms
        :log: whether to use log-scaling on the y-axis or not
        :align: how to align the bars, defaults to center.

    Returns:
        Matplotlib figure with the feature distributions

    Raises:
        :FeatureDistributionsNotComputed: if the feature distributions to visualize have not been computed.
    """
    stats = _do_get_featuregroup_statistics(featuregroup_name, featurestore=featurestore,
                                            featuregroup_version=featuregroup_version)
    if stats.feature_histograms is None or stats.feature_histograms.feature_distributions is None:
        raise FeatureDistributionsNotComputed("Cannot visualize the feature distributions for the "
                                              "feature group: {} with version: {} in featurestore: {} since the "
                                              "feature distributions have not been computed for this featuregroup."
                                              " To compute the feature distributions, call "
                                              "featurestore.update_featuregroup_stats(featuregroup_name)")
    fig = statistics_plots._visualize_feature_distributions(stats.feature_histograms.feature_distributions,
                                                            figsize=figsize, color=color, log=log, align=align)
    return fig


def _do_visualize_featuregroup_correlations(featuregroup_name, featurestore=None, featuregroup_version=1,
                                            figsize=(16, 12), cmap="coolwarm", annot=True, fmt=".2f", linewidths=.05):
    """
    Creates a matplotlib figure of the feature correlations in a featuregroup in the featurestore.

    1. Fetches the stored statistics for the featuregroup
    2. If the feature correlations have been computed for the featuregroup, create the figure

    Args:
        :featuregroup_name: the name of the featuregroup
        :featurestore: the featurestore where the featuregroup resides
        :featuregroup_version: the version of the featuregroup
        :figsize: the size of the figure
        :cmap: the color map
        :annot: whether to annotate the heatmap
        :fmt: how to format the annotations
        :linewidths: line width in the plot

    Returns:
        Matplotlib figure with the feature correlations

    Raises:
        :FeatureCorrelationsNotComputed: if the feature distributions to visualize have not been computed.
    """
    stats = _do_get_featuregroup_statistics(featuregroup_name, featurestore=featurestore,
                                            featuregroup_version=featuregroup_version)
    if stats.correlation_matrix is None or stats.correlation_matrix.feature_correlations is None:
        raise FeatureCorrelationsNotComputed("Cannot visualize the feature correlations for the "
                                             "feature group: {} with version: {} in featurestore: {} since the "
                                             "feature correlations have not been computed for this featuregroup."
                                             " To compute the feature correlations, call "
                                             "featurestore.update_featuregroup_stats(featuregroup_name)")
    fig = statistics_plots._visualize_feature_correlations(stats.correlation_matrix.feature_correlations,
                                                           figsize=figsize, cmap=cmap, annot=annot, fmt=fmt,
                                                           linewidths=linewidths)
    return fig


def _do_visualize_featuregroup_clusters(featuregroup_name, featurestore=None, featuregroup_version=1, figsize=(16, 12)):
    """
    Creates a matplotlib figure of the feature clusters in a featuregroup in the featurestore.

    1. Fetches the stored statistics for the featuregroup
    2. If the feature clusters have been computed for the featuregroup, create the figure

    Args:
        :featuregroup_name: the name of the featuregroup
        :featurestore: the featurestore where the featuregroup resides
        :featuregroup_version: the version of the featuregroup
        :figsize: the size of the figure

    Returns:
        Matplotlib figure with the feature clusters

    Raises:
        :FeatureClustersNotComputed: if the feature distributions to visualize have not been computed.
    """
    stats = _do_get_featuregroup_statistics(featuregroup_name, featurestore=featurestore,
                                            featuregroup_version=featuregroup_version)
    if stats.cluster_analysis is None:
        raise FeatureClustersNotComputed("Cannot visualize the feature clusters for the "
                                         "feature group: {} with version: {} in featurestore: {} since the "
                                         "feature clusters have not been computed for this featuregroup."
                                         " To compute the feature clusters, call "
                                         "featurestore.update_featuregroup_stats(featuregroup_name)")
    fig = statistics_plots._visualize_feature_clusters(stats.cluster_analysis, figsize=figsize)
    return fig


def _do_visualize_featuregroup_descriptive_stats(featuregroup_name, featurestore=None,
                                                 featuregroup_version=1):
    """
    Creates a pandas dataframe of the descriptive statistics of a featuregroup in the featurestore.

    1. Fetches the stored statistics for the featuregroup
    2. If the descriptive statistics have been computed for the featuregroup, create the pandas dataframe

    Args:
        :featuregroup_name: the name of the featuregroup
        :featurestore: the featurestore where the featuregroup resides
        :featuregroup_version: the version of the featuregroup

    Returns:
        Pandas dataframe with the descriptive statistics

    Raises:
        :DescriptiveStatisticsNotComputed: if the feature distributions to visualize have not been computed.
    """
    stats = _do_get_featuregroup_statistics(featuregroup_name, featurestore=featurestore,
                                            featuregroup_version=featuregroup_version)
    if stats.descriptive_stats is None or stats.descriptive_stats.descriptive_stats is None:
        raise DescriptiveStatisticsNotComputed("Cannot visualize the descriptive statistics for the "
                                               "featuregroup: {} with version: {} in featurestore: {} since the "
                                               "descriptive statistics have not been computed for this featuregroup."
                                               " To compute the descriptive statistics, call "
                                               "featurestore.update_featuregroup_stats(featuregroup_name)")
    df = statistics_plots._visualize_descriptive_stats(stats.descriptive_stats.descriptive_stats)
    return df


def _do_visualize_training_dataset_distributions(training_dataset_name, featurestore=None, training_dataset_version=1,
                                                 figsize=(16, 12), color='lightblue', log=False, align="center"):
    """
    Creates a matplotlib figure of the feature distributions in a training dataset in the featurestore.

    1. Fetches the stored statistics for the training dataset
    2. If the feature distributions have been computed for the training dataset, create the figure

    Args:
        :training_dataset_name: the name of the training dataset
        :featurestore: the featurestore where the training dataset resides
        :training_dataset_version: the version of the training dataset
        :figsize: size of the figure
        :color: the color of the histograms
        :log: whether to use log-scaling on the y-axis or not
        :align: how to align the bars, defaults to center.

    Returns:
        Matplotlib figure with the feature distributions

    Raises:
        :FeatureDistributionsNotComputed: if the feature distributions to visualize have not been computed.
    """
    stats = _do_get_training_dataset_statistics(training_dataset_name, featurestore=featurestore,
                                                training_dataset_version=training_dataset_version)
    if stats.feature_histograms is None or stats.feature_histograms.feature_distributions is None:
        raise FeatureDistributionsNotComputed("Cannot visualize the feature distributions for the "
                                              "training dataset: {} with version: {} in featurestore: {} since the "
                                              "feature distributions have not been computed for this training dataset."
                                              " To compute the feature distributions, call "
                                              "featurestore.update_training_dataset_stats(training_dataset_name)")
    fig = statistics_plots._visualize_feature_distributions(stats.feature_histograms.feature_distributions,
                                                            figsize=figsize, color=color, log=log, align=align)
    return fig


def _do_visualize_training_dataset_correlations(training_dataset_name, featurestore=None, training_dataset_version=1,
                                                figsize=(16, 12), cmap="coolwarm", annot=True, fmt=".2f",
                                                linewidths=.05):
    """
    Creates a matplotlib figure of the feature correlations in a training dataset in the featurestore.

    1. Fetches the stored statistics for the training dataset
    2. If the feature correlations have been computed for the training dataset, create the figure

    Args:
        :training_dataset_name: the name of the training dataset
        :featurestore: the featurestore where the training dataset resides
        :tranining_dataset_version: the version of the training dataset
        :figsize: the size of the figure
        :cmap: the color map
        :annot: whether to annotate the heatmap
        :fmt: how to format the annotations
        :linewidths: line width in the plot

    Returns:
        Matplotlib figure with the feature correlations

    Raises:
        :FeatureCorrelationsNotComputed: if the feature distributions to visualize have not been computed.
    """
    stats = _do_get_training_dataset_statistics(training_dataset_name, featurestore=featurestore,
                                                training_dataset_version=training_dataset_version)
    if stats.correlation_matrix is None or stats.correlation_matrix.feature_correlations is None:
        raise FeatureCorrelationsNotComputed("Cannot visualize the feature correlations for the "
                                             "training dataset: {} with version: {} in featurestore: {} since the "
                                             "feature correlations have not been computed for this training dataset."
                                             " To compute the feature correlations, call "
                                             "featurestore.update_training_dataset_stats(training_dataset_name)")
    fig = statistics_plots._visualize_feature_correlations(stats.correlation_matrix.feature_correlations,
                                                           figsize=figsize, cmap=cmap, annot=annot, fmt=fmt,
                                                           linewidths=linewidths)
    return fig


def _do_visualize_training_dataset_clusters(training_dataset_name, featurestore=None, training_dataset_version=1,
                                            figsize=(16, 12)):
    """
    Creates a matplotlib figure of the feature clusters in a training dataset in the featurestore.

    1. Fetches the stored statistics for the training dataset
    2. If the feature clusters have been computed for the training dataset, create the figure

    Args:
        :training_dataset_name: the name of the training dataset
        :featurestore: the featurestore where the training dataset resides
        :training_dataset_version: the version of the training dataset
        :figsize: the size of the figure

    Returns:
        Matplotlib figure with the feature clusters

    Raises:
        :FeatureClustersNotComputed: if the feature distributions to visualize have not been computed.
    """
    stats = _do_get_training_dataset_statistics(training_dataset_name, featurestore=featurestore,
                                                training_dataset_version=training_dataset_version)
    if stats.cluster_analysis is None:
        raise FeatureClustersNotComputed("Cannot visualize the feature clusters for the "
                                         "training dataset: {} with version: {} in featurestore: {} since the "
                                         "feature clusters have not been computed for this training dataset."
                                         " To compute the feature clusters, call "
                                         "featurestore.update_training_dataset_stats(training_dataset_name)")
    fig = statistics_plots._visualize_feature_clusters(stats.cluster_analysis, figsize=figsize)
    return fig


def _do_visualize_training_dataset_descriptive_stats(training_dataset_name, featurestore=None,
                                                     training_dataset_version=1):
    """
    Creates a pandas dataframe of the descriptive statistics of a training dataset in the featurestore.

    1. Fetches the stored statistics for the training dataset
    2. If the descriptive statistics have been computed for the training dataset, create the pandas dataframe

    Args:
        :training_dataset_name: the name of the training dataset
        :featurestore: the featurestore where the training dataset resides
        :training_dataset_version: the version of the training dataset

    Returns:
        Pandas dataframe with the descriptive statistics

    Raises:
        :DescriptiveStatisticsNotComputed: if the feature distributions to visualize have not been computed.
    """
    stats = _do_get_training_dataset_statistics(training_dataset_name, featurestore=featurestore,
                                                training_dataset_version=training_dataset_version)
    if stats.descriptive_stats is None or stats.descriptive_stats.descriptive_stats is None:
        raise DescriptiveStatisticsNotComputed("Cannot visualize the descriptive statistics for the "
                                               "training dataset: {} with version: {} in featurestore: {} since the "
                                               "descriptive statistics have not been computed for this training dataset."
                                               " To compute the descriptive statistics, call "
                                               "featurestore.update_training_dataset_stats(training_dataset_name)")
    df = statistics_plots._visualize_descriptive_stats(stats.descriptive_stats.descriptive_stats)
    return df


def _do_get_featuregroup_statistics(featuregroup_name, featurestore=None, featuregroup_version=1):
    """
    Gets the computed statistics (if any) of a featuregroup

    Args:
        :featuregroup_name: the name of the featuregroup
        :featurestore: the featurestore where the featuregroup resides
        :featuregroup_version: the version of the featuregroup

    Returns:
          A Statistics Object
    """
    featuregroup_id = _get_featuregroup_id(featurestore, featuregroup_name, featuregroup_version)
    featurestore_id = _get_featurestore_id(featurestore)
    response_object = rest_rpc._get_featuregroup_rest(featuregroup_id, featurestore_id)
    # .get() returns None if key dont exists intead of exception
    descriptive_stats_json = response_object.get(constants.REST_CONFIG.JSON_FEATUREGROUP_DESC_STATS)
    correlation_matrix_json = response_object.get(constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURE_CORRELATION)
    features_histogram_json = response_object.get(constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES_HISTOGRAM)
    feature_clusters = response_object.get(constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES_CLUSTERS)
    return Statistics(descriptive_stats_json, correlation_matrix_json, features_histogram_json, feature_clusters)


def _do_get_training_dataset_statistics(training_dataset_name, featurestore=None, training_dataset_version=1):
    """
    Gets the computed statistics (if any) of a training dataset

    Args:
        :training_dataset_name: the name of the training dataset
        :featurestore: the featurestore where the training dataset resides
        :training_dataset_version: the version of the training dataset

    Returns:
          A Statistics Object
    """
    training_dataset_id = _get_training_dataset_id(featurestore, training_dataset_name, training_dataset_version)
    featurestore_id = _get_featurestore_id(featurestore)
    response_object = rest_rpc._get_training_dataset_rest(training_dataset_id, featurestore_id)
    # .get() returns None if key dont exists intead of exception
    descriptive_stats_json = response_object.get(constants.REST_CONFIG.JSON_FEATUREGROUP_DESC_STATS)
    correlation_matrix_json = response_object.get(constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURE_CORRELATION)
    features_histogram_json = response_object.get(constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES_HISTOGRAM)
    feature_clusters = response_object.get(constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES_CLUSTERS)
    return Statistics(descriptive_stats_json, correlation_matrix_json, features_histogram_json, feature_clusters)


def _verify_hive_enabled(spark):
    """
    Verifies that Hive is enabled on the given spark session.

    Args:
        :spark: the spark session to verfiy

    Returns:
         None

    Raises:
        :HiveNotEnabled: when hive is not enabled on the provided spark session
    """
    if not fs_utils._is_hive_enabled(spark):
        raise HiveNotEnabled((
            "Hopsworks Featurestore Depends on Hive. Hive is not enabled for the current spark session. "
            "Make sure to enable hive before using the featurestore API."
            " The current SparkSQL catalog implementation is: {}, it should be: {}".format(
                fs_utils._get_spark_sql_catalog_impl(spark), constants.SPARK_CONFIG.SPARK_SQL_CATALOG_HIVE)))



def _sync_hive_table_with_featurestore(featuregroup, featurestore_metadata, description="", featurestore=None,
                                       featuregroup_version=1, jobs=[], feature_corr_data = None,
                                       featuregroup_desc_stats_data = None, features_histogram_data = None,
                                       cluster_analysis_data = None):
    """
    Synchronizes an existing Hive table with a Feature Store.

    Args:
        :featuregroup: name of the featuregroup to synchronize with the hive table.
                       The hive table should have a naming scheme of `featuregroup_featuregroupversion`
        :featurestore_metadata: metadata of the feature store
        :description: description of the feature group
        :featurestore: the feature store where the hive table is stored
        :featuregroup_version: version of the feature group
        :jobs: list of jobs used to compute the feature group (optional)
        :feature_corr_data: feature correlation statistics (optional)
        :featuregroup_desc_stats_data: descriptive statistics (optional)
        :features_histogram_data: feature histograms statistics (optional)
        :cluster_analysis_data: cluster analysis statistics (optional)

    Returns:
        None

    """
    featuregroup_type, featuregroup_type_dto = fs_utils._get_cached_featuregroup_type_info(featurestore_metadata)
    featurestore_id = _get_featurestore_id(featurestore)
    rest_rpc._sync_hive_table_with_featurestore_rest(featuregroup, featurestore_id, description, featuregroup_version, jobs,
                                                     feature_corr_data, featuregroup_desc_stats_data, features_histogram_data,
                                                     cluster_analysis_data, featuregroup_type, featuregroup_type_dto)


def _do_get_s3_featuregroup(storage_connector_name, dataset_path, featurestore_metadata,
                            featurestore=None, data_format="parquet"):
    """
    Gets a feature dataset stored externally (e.g in a S3 bucket)

    Args:
        :storage_connector_name: the storage connector to the external data store
        :dataset_path: the path to the dataset in the external datastore
        :featurestore_metadata: featurestore metadata
        :featurestore: the featurestore of the storage connector
        :data_format: the data format (so that we know how to read the dataset)

    Returns:
        A spark dataframe of the external feature group
    """
    storage_connector = _do_get_storage_connector(storage_connector_name, featurestore)
    if storage_connector.type == featurestore_metadata.settings.s3_connector_type:
        if storage_connector.access_key is not None:
            fs_utils._setup_s3_credentials_for_spark(storage_connector.access_key,
                                                    storage_connector.secret_key, util._find_spark())
        path = fs_utils._get_bucket_path(storage_connector.bucket, dataset_path)
        featureframe = FeatureFrame.get_featureframe(path=path,
                                                     dataframe_type=constants.FEATURE_STORE.DATAFRAME_TYPE_SPARK,
                                                     data_format=data_format)
        return featureframe.read_featureframe(util._find_spark())
    elif storage_connector.type not in metadata_cache.settings.feature_import_connectors:
        raise StorageConnectorTypeNotSupportedForFeatureImport("The storage conector type: {} is not supported for "
                                                                   "feature importation. Supported feature storage "
                                                                   "connectors for importation are: " \
                                                                   .format(storage_connector.type))


def _do_get_online_featurestore_connector(featurestore, featurestore_metadata):
    """
    Gets the JDBC connector for the online featurestore

    Args:
        :featurestore: the featurestore name
        :featurestore_metadata: the featurestore metadata

    Returns:
        a JDBC connector DTO object for the online featurestore
    """

    if featurestore_metadata is not None and featurestore_metadata.online_featurestore_connector is not None:
        return featurestore_metadata.online_featurestore_connector
    else:
        featurestore_id = _get_featurestore_id(featurestore)
        response_object = rest_rpc._get_online_featurestore_jdbc_connector_rest(featurestore_id)
        return JDBCStorageConnector(response_object)


def _do_create_featuregroup(df, featurestore_metadata, featuregroup, primary_key=None, description="", featurestore=None,
                            featuregroup_version=1, jobs=[],
                            descriptive_statistics=True, feature_correlation=True,
                            feature_histograms=True, cluster_analysis=True, stat_columns=None, num_bins=20,
                            corr_method='pearson', num_clusters=5, partition_by=[], online=False,
                            online_types=None, offline=True):
    """
    Creates a new cached featuregroup from a dataframe of features (sends the metadata to Hopsworks with a REST call
    to create the Hive table and store the metadata and then inserts the data of the spark dataframe into the newly
    created table) If online=True, a MySQL table will be created for online feature data

    Args:
        :df: the dataframe to create the featuregroup for (used to infer the schema)
        :featuregroup: the name of the new featuregroup
        :primary_key: the primary key of the new featuregroup, if not specified, the first column in the dataframe will
                      be used as primary
        :description: a description of the featuregroup
        :featurestore: the featurestore of the featuregroup (defaults to the project's featurestore)
        :featuregroup_version: the version of the featuregroup (defaults to 1)
        :jobs: list of Hopsworks jobs linked to the feature group
        :descriptive_statistics: a boolean flag whether to compute descriptive statistics (min,max,mean etc) for the
                                 featuregroup
        :feature_correlation: a boolean flag whether to compute a feature correlation matrix for the numeric columns in
                              the featuregroup
        :feature_histograms: a boolean flag whether to compute histograms for the numeric columns in the featuregroup
        :cluster_analysis: a boolean flag whether to compute cluster analysis for the numeric columns in the
                           featuregroup
        :stat_columns: a list of columns to compute statistics for (defaults to all columns that are numeric)
        :num_bins: number of bins to use for computing histograms
        :num_clusters: the number of clusters to use for cluster analysis
        :corr_method: the method to compute feature correlation with (pearson or spearman)
        :partition_by: a list of columns to partition_by, defaults to the empty list
        :online: boolean flag, if this is set to true, a MySQL table for online feature data will be created in
                 addition to the Hive table for offline feature data
        :online_types: a dict with feature_name --> online_type, if a feature is present in this dict,
                            the online_type will be taken from the dict rather than inferred from the spark dataframe.
        :offline boolean flag whether to insert the data in the offline version of the featuregroup

    Returns:
        None

    Raises:
        :CouldNotConvertDataframe: in case the provided dataframe could not be converted to a spark dataframe
    """
    if online and ((not featurestore_metadata.settings.online_enabled) or
                       (not featurestore_metadata.featurestore.online_enabled)):
        raise OnlineFeaturestoreNotEnabled("Online Feature Store is not enabled for this project or cluster, "
                                           "talk to an administrator to enable it")
    try:
        spark_df = fs_utils._convert_dataframe_to_spark(df)
    except Exception as e:
        raise CouldNotConvertDataframe(
            "Could not convert the provided dataframe to a spark dataframe which is required in order to save it to "
            "the Feature Store, error: {}".format(
                str(e)))

    fs_utils._validate_metadata(featuregroup, spark_df.dtypes, description)

    if primary_key is None:
        primary_key = fs_utils._get_default_primary_key(spark_df)
    if util.get_job_name() is not None:
        jobs.append(util.get_job_name())

    fs_utils._validate_primary_key(spark_df, primary_key)
    features_schema = _parse_spark_features_schema(spark_df.schema, primary_key, partition_by, online=online,
                                                   online_types= online_types)
    feature_corr_data, featuregroup_desc_stats_data, features_histogram_data, cluster_analysis_data = \
        _compute_dataframe_stats(
            spark_df, featuregroup, version=featuregroup_version,
            descriptive_statistics=descriptive_statistics, feature_correlation=feature_correlation,
            feature_histograms=feature_histograms, cluster_analysis=cluster_analysis, stat_columns=stat_columns,
            num_bins=num_bins,
            corr_method=corr_method,
            num_clusters=num_clusters)
    featurestore_id = _get_featurestore_id(featurestore)
    featuregroup_type, featuregroup_type_dto = fs_utils._get_cached_featuregroup_type_info(featurestore_metadata)
    fs_utils._log("Registering feature metadata...")
    rest_rpc._create_featuregroup_rest(featuregroup, featurestore_id, description, featuregroup_version, jobs,
                                       features_schema, feature_corr_data, featuregroup_desc_stats_data,
                                       features_histogram_data, cluster_analysis_data, featuregroup_type,
                                       featuregroup_type_dto, None, None, online)
    fs_utils._log("Registering feature metadata... [COMPLETE]")
    if offline:
        fs_utils._log("Writing feature data to offline feature group (Hive)...")
        _write_featuregroup_hive(spark_df, featuregroup, featurestore, featuregroup_version,
                                      constants.FEATURE_STORE.FEATURE_GROUP_INSERT_APPEND_MODE)
        fs_utils._log("Writing feature data to offline feature group (Hive)... [COMPLETE]")
    if online:
        fs_utils._log("Writing feature data to online feature group (MySQL)...")
        storage_connector = _do_get_online_featurestore_connector(featurestore, featurestore_metadata)
        tbl_name = fs_utils._get_table_name(featuregroup, featuregroup_version)
        online_featurestore._write_jdbc_dataframe(spark_df, storage_connector, tbl_name,
                                                  write_mode=constants.FEATURE_STORE.FEATURE_GROUP_INSERT_APPEND_MODE)
        fs_utils._log("Writing feature data to online feature group (MySQL)... [COMPLETE]")


def _do_enable_featuregroup_online(featuregroup_name, featuregroup_version, featurestore_metadata, featurestore=None,
                                   online_types=None):
    """
    Enables online serving for a featuregroup

    Args:
        :featuregroup_name: name of the featuregroup
        :featuregroup_version: version of the featuregroup
        :featurestore_metadata: metadata about the featurestore
        :featurestore: the featurestore to query
        :online_types: a dict with feature_name --> online_type, if a feature is present in this dict,
                            the online_type will be taken from the dict rather than inferred from the spark dataframe.

    Returns:
        None
    """
    if featurestore is None:
        featurestore = fs_utils._do_get_project_featurestore()
    fg = query_planner._find_featuregroup(featurestore_metadata.featuregroups, featuregroup_name, featuregroup_version)
    if fg.featuregroup_type == featurestore_metadata.settings.on_demand_featuregroup_type:
        raise CannotEnableOnlineFeatureServingForOnDemandFeatureGroup("The feature group with name: {} , and version: {} "
                                                           "is an on-demand feature group. Online feature serving "
                                                            "operation is only supported for cached feature groups."
                                                           .format(featuregroup_name, featuregroup_version))

    spark_df = _do_get_cached_featuregroup(featuregroup_name, featurestore, featuregroup_version,
                                       constants.FEATURE_STORE.DATAFRAME_TYPE_SPARK,online=False)
    primary_key = None
    for feature in fg.features:
        if feature.primary:
            primary_key = feature.name
    features_schema = _parse_spark_features_schema(spark_df.schema, primary_key, [], online=True,
                                                   online_types=online_types)
    featuregroup_id = fg.id
    featurestore_id = featurestore_metadata.featurestore.id
    featuregroup_type, featuregroup_type_dto = fs_utils._get_cached_featuregroup_type_info(featurestore_metadata)
    rest_rpc._enable_featuregroup_online_rest(featuregroup_name, featuregroup_version, featuregroup_id,
                                              featurestore_id, featuregroup_type_dto, featuregroup_type,
                                              features_schema)


def _do_disable_featuregroup_online(featuregroup_name, featuregroup_version, featurestore_metadata):
    """
    Disable online serving for a featuregroup

    Args:
        :featuregroup_name: name of the featuregroup
        :featuregroup_version: version of the featuregroup
        :featurestore_metadata: metadata about the featurestore

    Returns:
        None
    """
    fg = query_planner._find_featuregroup(featurestore_metadata.featuregroups, featuregroup_name, featuregroup_version)
    if fg.featuregroup_type == featurestore_metadata.settings.on_demand_featuregroup_type:
        raise CannotEnableOnlineFeatureServingForOnDemandFeatureGroup("The feature group with name: {} , and version: {} "
                                                                      "is an on-demand feature group. Online feature serving "
                                                                      "operation is only supported for cached feature groups."
                                                                      .format(featuregroup_name, featuregroup_version))
    featuregroup_id = fg.id
    featurestore_id = featurestore_metadata.featurestore.id
    featuregroup_type, featuregroup_type_dto = fs_utils._get_cached_featuregroup_type_info(featurestore_metadata)
    rest_rpc._disable_featuregroup_online_rest(featuregroup_name, featuregroup_version, featuregroup_id,
                                              featurestore_id, featuregroup_type_dto, featuregroup_type)



def _do_get_redshift_featuregroup(storage_connector_name, query, featurestore_metadata, featurestore):
    """
    Gets a feature dataset stored in Redshift

    Args:
        :storage_connector_name: the storage connector to the external data store
        :query: the query extracting the data from Redshift
        :featurestore_metadata: featurestore metadata
        :featurestore: the featurestore of the storage connector

    Returns:
        A spark dataframe of the Redshift feature group
    """
    storage_connector = _do_get_storage_connector(storage_connector_name, featurestore)
    if storage_connector.type == featurestore_metadata.settings.jdbc_connector_type:
        region_name, cluster_identifier, database, user = util.parse_redhift_jdbc_url(
            storage_connector.connection_string + '?' + storage_connector.arguments)
        user, password = util.get_redshift_username_password(region_name, cluster_identifier, user, database)

        return util._find_spark().read.format(constants.SPARK_CONFIG.SPARK_JDBC_FORMAT) \
            .option(constants.SPARK_CONFIG.SPARK_JDBC_URL, storage_connector.connection_string) \
            .option(constants.SPARK_CONFIG.SPARK_JDBC_DBTABLE, '(' + query + ') tmp') \
            .option(constants.SPARK_CONFIG.SPARK_JDBC_USER, user) \
            .option(constants.SPARK_CONFIG.SPARK_JDBC_PW, password) \
            .load()
    elif storage_connector.type not in metadata_cache.settings.feature_import_connectors:
        raise StorageConnectorTypeNotSupportedForFeatureImport("The storage conector type: {} is not supported for "
                                                                   "feature importation. Supported feature storage "
                                                                   "connectors for importation are: " \
                                                                   .format(storage_connector.type))


# Fetch on-load and cache it on the client
try:
    metadata_cache = _get_featurestore_metadata(featurestore=fs_utils._do_get_project_featurestore())
except:
    pass
