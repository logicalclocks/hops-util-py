"""
Featurestore Core Implementation

Module hierarchy of featurestore implementation:

              					      +---+
                              +-------+dao|
 				              |	      +---+
 				              |
                              |       +-----------+
                              +-------+exceptions |
                              |       +-----------+
	   +--------------+	      |
	   |              |    +--+---+	  +-------------+
	   | featurestore +----+ core +---+query_planner|
	   |              |    +---+--+	  +-------------+
	   +--------------+	       |
				               |	  +----+
                               +------+rest|
				               |	  +----+
				               |
				               |	  +----+
                               +------+util|
					           |      +----+
					           |
					           |      +---------------+
					           +------+ featureframes |
					           |      +---------------+
"""

from hops import constants, util, hdfs
from hops.featurestore_impl.rest import rest_rpc
from hops.featurestore_impl.exceptions.exceptions import FeaturegroupNotFound, HiveDatabaseNotFound, \
    TrainingDatasetNotFound, CouldNotConvertDataframe, TFRecordSchemaNotFound
from hops.featurestore_impl.dao.featurestore_metadata import FeaturestoreMetadata
from hops.featurestore_impl.dao.training_dataset import TrainingDataset
from hops.featurestore_impl.query_planner.logical_query_plan import LogicalQueryPlan
from hops.featurestore_impl.query_planner.f_query import FeatureQuery, FeaturesQuery
from hops.featurestore_impl.query_planner.fg_query import FeaturegroupQuery
from hops.featurestore_impl.query_planner import query_planner
from hops.featurestore_impl.util import fs_utils
from hops.featurestore_impl.featureframes.FeatureFrame import FeatureFrame
import pydoop.hdfs as pydoop
import json

# for backwards compatibility
try:
    import h5py
except:
    pass

# in case importing in %%local
try:
    from pyspark.sql import SQLContext
    from pyspark.sql.utils import AnalysisException
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


def _convert_field_to_feature_json(field_dict, primary_key, partition_by):
    """
    Helper function that converts a field in a spark dataframe to a feature dict that is compatible with the
     featurestore API

    Args:
        :field_dict: the dict of spark field to convert
        :primary_key: name of the primary key feature
        :partition_by: a list of columns to partition_by, defaults to the empty list

    Returns:
        a feature dict that is compatible with the featurestore API

    """
    f_name = field_dict[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELD_NAME]
    f_type = fs_utils._convert_spark_dtype_to_hive_dtype(field_dict[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELD_TYPE])
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
        constants.REST_CONFIG.JSON_FEATURE_PARTITION: f_partition
    }


def _parse_spark_features_schema(spark_schema, primary_key, partition_by = []):
    """
    Helper function for parsing the schema of a spark dataframe into a list of feature-dicts

    Args:
        :spark_schema: the spark schema to parse
        :primary_key: the column in the dataframe that should be the primary key
        :partition_by: a list of columns to partition_by, defaults to the empty list

    Returns:
        A list of the parsed features

    """
    raw_schema = json.loads(spark_schema.json())
    raw_fields = raw_schema[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELDS]
    parsed_features = list(map(lambda field: _convert_field_to_feature_json(field, primary_key, partition_by),
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


def _do_get_feature(feature, featurestore_metadata, featurestore=None, featuregroup=None, featuregroup_version=1,
                    dataframe_type="spark"):
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

    Returns:
        A spark dataframe with the feature

    """
    spark = util._find_spark()
    _use_featurestore(spark, featurestore)
    spark.sparkContext.setJobGroup("Fetching Feature",
                                   "Getting feature: {} from the featurestore {}".format(feature, featurestore))

    feature_query = FeatureQuery(feature, featurestore_metadata, featurestore, featuregroup, featuregroup_version)
    logical_query_plan = LogicalQueryPlan(feature_query)
    logical_query_plan.create_logical_plan()
    logical_query_plan.construct_sql()

    result = _run_and_log_sql(spark, logical_query_plan.sql_str)
    spark.sparkContext.setJobGroup("", "")
    return fs_utils._return_dataframe_type(result, dataframe_type)


def _run_and_log_sql(spark, sql_str):
    """
    Runs and logs an SQL query with sparkSQL

    Args:
        :spark: the spark session
        :sql_str: the query to run

    Returns:
        the result of the SQL query
    """
    fs_utils._log("Running sql: {}".format(sql_str))
    return spark.sql(sql_str)


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
    """
    spark = util._find_spark()
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


def _do_get_features(features, featurestore_metadata, featurestore=None, featuregroups_version_dict={}, join_key=None,
                     dataframe_type="spark"):
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

    Returns:
        A spark dataframe with all the features

    """
    spark = util._find_spark()
    _use_featurestore(spark, featurestore)
    spark.sparkContext.setJobGroup("Fetching Features",
                                   "Getting features: {} from the featurestore {}".format(features, featurestore))

    features_query = FeaturesQuery(features, featurestore_metadata, featurestore, featuregroups_version_dict, join_key)
    logical_query_plan = LogicalQueryPlan(features_query)
    logical_query_plan.create_logical_plan()
    logical_query_plan.construct_sql()

    result = _run_and_log_sql(spark, logical_query_plan.sql_str)
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
    _get_featurestore_metadata(featurestore, update_cache=True)
    return response_object


def _do_get_featuregroup(featuregroup, featurestore=None, featuregroup_version=1, dataframe_type="spark"):
    """
    Gets a featuregroup from a featurestore as a spark dataframe

    Args:
        :featuregroup: the featuregroup to get
        :featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        :featuregroup_version: (Optional) the version of the featuregroup
        :dataframe_type: the type of the returned dataframe (spark, pandas, python or numpy)

    Returns:
        a spark dataframe with the contents of the featurestore

    """
    spark = util._find_spark()
    _use_featurestore(spark, featurestore)
    spark.sparkContext.setJobGroup("Fetching Featuregroup",
                                   "Getting feature group: {} from the featurestore {}".format(featuregroup,
                                                                                               featurestore))
    featuregroup_query = FeaturegroupQuery(featuregroup, featurestore, featuregroup_version)
    logical_query_plan = LogicalQueryPlan(featuregroup_query)
    logical_query_plan.create_logical_plan()
    logical_query_plan.construct_sql()

    result = _run_and_log_sql(spark, logical_query_plan.sql_str)
    spark.sparkContext.setJobGroup("", "")
    return fs_utils._return_dataframe_type(result, dataframe_type)


def _do_get_training_dataset(training_dataset_name, featurestore_metadata, training_dataset_version=1,
                             dataframe_type="spark"):
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

    training_dataset = query_planner._find_training_dataset(featurestore_metadata.training_datasets,
                                                            training_dataset_name,
                                                            training_dataset_version)
    hdfs_path = training_dataset.hdfs_path + \
                constants.DELIMITERS.SLASH_DELIMITER + training_dataset.name
    data_format = training_dataset.data_format
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_IMAGE_FORMAT:
        hdfs_path = training_dataset.hdfs_path
    # abspath means "hdfs://namenode:port/ is preprended
    abspath = pydoop.path.abspath(hdfs_path)

    featureframe = FeatureFrame.get_featureframe(path=abspath, dataframe_type=dataframe_type,
                                                 data_format=data_format, training_dataset=training_dataset_name)
    return featureframe.read_featureframe()


def _do_create_training_dataset(df, training_dataset, description="", featurestore=None,
                                data_format="tfrecords", training_dataset_version=1,
                                job_name=None, dependencies=[], descriptive_statistics=True, feature_correlation=True,
                                feature_histograms=True, cluster_analysis=True, stat_columns=None, num_bins=20,
                                corr_method='pearson', num_clusters=5, petastorm_args={}, fixed=True):
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
        :job_name: the name of the job to compute the training dataset
        :dependencies: list of the datasets that this training dataset depends on (e.g input datasets to the
                        feature engineering job)
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

    Returns:
        None
    """
    try:
        spark_df = fs_utils._convert_dataframe_to_spark(df)
    except Exception as e:
        raise CouldNotConvertDataframe(
            "Could not convert the provided dataframe to a spark dataframe which is required in order "
            "to save it to the Feature Store, error: {}".format(
                str(e)))

    fs_utils._validate_metadata(training_dataset, spark_df.dtypes, dependencies, description)

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
    td_json = rest_rpc._create_training_dataset_rest(
        training_dataset, featurestore_id, description, training_dataset_version,
        data_format, job_name, dependencies, features_schema,
        feature_corr_data, training_dataset_desc_stats_data, features_histogram_data, cluster_analysis_data)
    hdfs_path = pydoop.path.abspath(td_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_HDFS_STORE_PATH])
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT:
        try:
            tf_record_schema_json = fs_utils._get_dataframe_tf_record_schema_json(spark_df, fixed=fixed)[1]
            fs_utils._store_tf_record_schema_hdfs(tf_record_schema_json, hdfs_path)
        except Exception as e:
            fs_utils._log("Could not infer tfrecords schema for the dataframe, {}".format(str(e)))

    featureframe = FeatureFrame.get_featureframe(path=hdfs_path +
                                                      constants.DELIMITERS.SLASH_DELIMITER + training_dataset,
                                                 data_format=data_format, df=spark_df,
                                                 write_mode=constants.SPARK_CONFIG.SPARK_OVERWRITE_MODE,
                                                 training_dataset=training_dataset,
                                                 petastorm_args=petastorm_args)
    spark = util._find_spark()
    spark.sparkContext.setJobGroup("Materializing dataframe as training dataset",
                                   "Saving training dataset in path: {} in format {}".format(hdfs_path, data_format))
    featureframe.write_featureframe()
    spark.sparkContext.setJobGroup("", "")
    # update metadata cache
    _get_featurestore_metadata(featurestore, update_cache=True)
    fs_utils._log("Training Dataset created successfully")


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

    """
    try:
        spark_df = fs_utils._convert_dataframe_to_spark(df)
    except Exception as e:
        raise CouldNotConvertDataframe(
            "Could not convert the provided dataframe to a spark dataframe which is required in order to save it to "
            "the Feature Store, error: {}".format(str(e)))

    if featurestore is None:
        featurestore = fs_utils._do_get_project_featurestore()
    training_dataset = query_planner._find_training_dataset(featurestore_metadata.training_datasets,
                                                            training_dataset_name, training_dataset_version)
    feature_corr_data, training_dataset_desc_stats_data, features_histogram_data, cluster_analysis_data = \
        _compute_dataframe_stats(
            spark_df, training_dataset_name, version=training_dataset_version,
            descriptive_statistics=descriptive_statistics, feature_correlation=feature_correlation,
            feature_histograms=feature_histograms, cluster_analysis=cluster_analysis, stat_columns=stat_columns,
            num_bins=num_bins, corr_method=corr_method,
            num_clusters=num_clusters)
    features_schema = _parse_spark_features_schema(spark_df.schema, None)
    training_dataset_id = _get_training_dataset_id(featurestore, training_dataset_name, training_dataset_version)
    featurestore_id = _get_featurestore_id(featurestore)
    td = TrainingDataset(rest_rpc._update_training_dataset_stats_rest(
        training_dataset_name, training_dataset_id, featurestore_id, training_dataset_version,
        features_schema, feature_corr_data, training_dataset_desc_stats_data, features_histogram_data,
        cluster_analysis_data))
    hdfs_path = pydoop.path.abspath(td.hdfs_path)
    data_format = training_dataset.data_format
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT:
        try:
            tf_record_schema_json = fs_utils._get_dataframe_tf_record_schema_json(spark_df, fixed)[1]
            fs_utils._store_tf_record_schema_hdfs(tf_record_schema_json, hdfs_path)
        except Exception as e:
            fs_utils._log("Could not infer tfrecords schema for the dataframe, {}".format(str(e)))
    featureframe = FeatureFrame.get_featureframe(path=hdfs_path +
                                                      constants.DELIMITERS.SLASH_DELIMITER + training_dataset_name,
                                                 data_format=data_format, df=spark_df, write_mode=write_mode,
                                                 training_dataset=training_dataset)
    spark = util._find_spark()
    spark.sparkContext.setJobGroup("Materializing dataframe as training dataset",
                                   "Saving training dataset in path: {} in format {}".format(hdfs_path, data_format))
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
    hdfs_path = training_dataset.hdfs_path + \
                constants.DELIMITERS.SLASH_DELIMITER + training_dataset.name
    data_format = training_dataset.data_format
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_NPY_FORMAT:
        hdfs_path = hdfs_path + constants.FEATURE_STORE.TRAINING_DATASET_NPY_SUFFIX
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_HDF5_FORMAT:
        hdfs_path = hdfs_path + constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_IMAGE_FORMAT:
        hdfs_path = training_dataset.hdfs_path
    # abspath means "hdfs://namenode:port/ is preprended
    abspath = pydoop.path.abspath(hdfs_path)
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
    hdfs_path = pydoop.path.abspath(training_dataset.hdfs_path)
    tf_record_json_schema = json.loads(hdfs.load(
        hdfs_path + constants.DELIMITERS.SLASH_DELIMITER +
        constants.FEATURE_STORE.TRAINING_DATASET_TF_RECORD_SCHEMA_FILE_NAME))
    return fs_utils._convert_tf_record_schema_json_to_dict(tf_record_json_schema)

def _do_get_featuregroup_partitions(featuregroup, featurestore=None, featuregroup_version=1, dataframe_type="spark"):
    """
    Gets the partitions of a featuregroup

     Args:
        :featuregroup: the featuregroup to get partitions for
        :featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        :featuregroup_version: the version of the featuregroup, defaults to 1
        :dataframe_type: the type of the returned dataframe (spark, pandas, python or numpy)
     Returns:
        a dataframe with the partitions of the featuregroup
     """
    spark = util._find_spark()
    spark.sparkContext.setJobGroup("Fetching Partitions of a Featuregroup",
                                   "Getting partitions for feature group: {} from the featurestore {}".format(
                                       featuregroup, featurestore))
    _use_featurestore(spark, featurestore)
    sql_str = "SHOW PARTITIONS " + fs_utils._get_table_name(featuregroup, featuregroup_version)
    result = _run_and_log_sql(spark, sql_str)
    spark.sparkContext.setJobGroup("", "")
    return fs_utils._return_dataframe_type(result, dataframe_type)

try:
    metadata_cache = _get_featurestore_metadata(featurestore=fs_utils._do_get_project_featurestore())
except:
    pass