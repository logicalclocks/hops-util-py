"""
A feature store client. This module exposes an API for interacting with feature stores in Hopsworks.
It hides complexity and provides utility methods such as:

    - `project_featurestore()`.
    - `get_featuregroup()`.
    - `get_feature()`.
    - `get_features()`.
    - `sql()`
    - `insert_into_featuregroup()`
    - `get_featurestore_metadata()`
    - `get_project_featurestores()`
    - `get_featuregroups()`
    - `get_training_datasets()`

Below is some example usages of this API (assuming you have two featuregroups called
'trx_graph_summary_features' and 'trx_summary_features' with schemas:

 |-- cust_id: integer (nullable = true)

 |-- pagerank: float (nullable = true)

 |-- triangle_count: float (nullable = true)

and

 |-- cust_id: integer (nullable = true)

 |-- min_trx: float (nullable = true)

 |-- max_trx: float (nullable = true)

 |-- avg_trx: float (nullable = true)

 |-- count_trx: long (nullable = true)

, respectively.

    >>> from hops import featurestore
    >>> # Get feature group example
    >>> #The API will default to version 1 for the feature group and the project's own feature store
    >>> trx_summary_features = featurestore.get_featuregroup("trx_summary_features")
    >>> #You can also explicitly define version and feature store:
    >>> trx_summary_features = featurestore.get_featuregroup("trx_summary_features",
    >>>                                                      featurestore=featurestore.project_featurestore(),
    >>>                                                      featuregroup_version = 1)
    >>>
    >>> # Get single feature example
    >>> #The API will infer the featuregroup and default to version 1 for the feature group with this and the project's
    >>> # own feature store
    >>> max_trx_feature = featurestore.get_feature("max_trx")
    >>> #You can also explicitly define feature group,version and feature store:
    >>> max_trx_feature = featurestore.get_feature("max_trx",
    >>>                                            featurestore=featurestore.project_featurestore(),
    >>>                                            featuregroup="trx_summary_features",
    >>>                                            featuregroup_version = 1)
    >>> # When you want to get features from different feature groups the API will infer how to join the features
    >>> # together
    >>>
    >>> # Get list of features example
    >>> # The API will default to version 1 for feature groups and the project's feature store
    >>> features = featurestore.get_features(["pagerank", "triangle_count", "avg_trx"],
    >>>                                      featurestore=featurestore.project_featurestore())
    >>> #You can also explicitly define feature group, version, feature store, and join-key:
    >>> features = featurestore.get_features(["pagerank", "triangle_count", "avg_trx"],
    >>>                                      featurestore=featurestore.project_featurestore(),
    >>>                                      featuregroups_version_dict={"trx_graph_summary_features": 1,
    >>>                                                                  "trx_summary_features": 1},
    >>>                                                                  join_key="cust_id")
    >>>
    >>> # Run SQL query against feature store example
    >>> # The API will default to the project's feature store
    >>> featurestore.sql("SELECT * FROM trx_graph_summary_features_1 WHERE triangle_count > 5").show(5)
    >>> # You can also explicitly define the feature store
    >>> featurestore.sql("SELECT * FROM trx_graph_summary_features_1 WHERE triangle_count > 5",
    >>>                  featurestore=featurestore.project_featurestore()).show(5)
    >>>
    >>> # Insert into featuregroup example
    >>> # The API will default to the project's feature store, featuegroup version 1, and write mode 'append'
    >>> featurestore.insert_into_featuregroup(sampleDf, "trx_graph_summary_features")
    >>> # You can also explicitly define the feature store, the featuregroup version, and the write mode
    >>> # (only append and overwrite are supported)
    >>> featurestore.insert_into_featuregroup(sampleDf, "trx_graph_summary_features",
    >>>                                      featurestore=featurestore.project_featurestore(),
    >>>                                      featuregroup_version=1, mode="append", descriptive_statistics=True,
    >>>                                      feature_correlation=True, feature_histograms=True, cluster_analysis=True,
    >>>                                      stat_columns=None)
    >>>
    >>> # Get featurestore metadata example
    >>> # The API will default to the project's feature store
    >>> featurestore.get_featurestore_metadata()
    >>> # You can also explicitly define the feature store
    >>> featurestore.get_featurestore_metadata(featurestore=featurestore.project_featurestore())
    >>>
    >>> # List all Feature Groups in a Feature Store
    >>> featurestore.get_featuregroups()
    >>> # By default `get_featuregroups()` will use the project's feature store, but this can also be
    >>> # specified with the optional argument `featurestore`
    >>> featurestore.get_featuregroups(featurestore=featurestore.project_featurestore())
    >>>
    >>> # List all Training Datasets in a Feature Store
    >>> featurestore.get_training_datasets()
    >>> # By default `get_training_datasets()` will use the project's feature store, but this can also be
    >>> # specified with the optional argument featurestore
    >>> featurestore.get_training_datasets(featurestore=featurestore.project_featurestore())
    >>>
    >>> # Get list of featurestores accessible by the current project example
    >>> featurestore.get_project_featurestores()
    >>> # By default `get_featurestore_metadata` will use the project's feature store, but this can also be
    >>> # specified with the optional argument featurestore
    >>> featurestore.get_featurestore_metadata(featurestore=featurestore.project_featurestore())
    >>>
    >>> # Compute featuergroup statistics (feature correlation, descriptive stats, feature distributions etc)
    >>> # with Spark that will show up in the Featurestore Registry in Hopsworks
    >>> # The API will default to the project's featurestore, featuregroup version 1, and
    >>> # compute all statistics for all columns
    >>> featurestore.update_featuregroup_stats("trx_summary_features")
    >>> # You can also be explicitly specify featuregroup details and what statistics to compute:
    >>> featurestore.update_featuregroup_stats("trx_summary_features", featuregroup_version=1,
    >>>                                        featurestore=featurestore.project_featurestore(),
    >>>                                        descriptive_statistics=True,feature_correlation=True,
    >>>                                        feature_histograms=True, cluster_analysis=True, stat_columns=None)
    >>> # If you only want to compute statistics for certain set of columns and exclude surrogate key-columns
    >>> # for example, you can use the optional argument stat_columns to specify which columns to include:
    >>> featurestore.update_featuregroup_stats("trx_summary_features", featuregroup_version=1,
    >>>                                        featurestore=featurestore.project_featurestore(),
    >>>                                        descriptive_statistics=True, feature_correlation=True,
    >>>                                        feature_histograms=True, cluster_analysis=True,
    >>>                                        stat_columns=['avg_trx', 'count_trx', 'max_trx', 'min_trx'])
    >>>
    >>> # Create featuregroup from an existing dataframe
    >>> # In most cases it is recommended that featuregroups are created in the UI on Hopsworks and that care is
    >>> # taken in documenting the featuregroup.
    >>> # However, sometimes it is practical to create a featuregroup directly from a spark dataframe and
    >>> # fill in the metadata about the featuregroup later in the UI.
    >>> # This can be done through the create_featuregroup API function
    >>>
    >>> # By default the new featuregroup will be created in the project's featurestore and the statistics for
    >>> # the new featuregroup will be computed based on the provided spark dataframe.
    >>> featurestore.create_featuregroup(trx_summary_df1, "trx_summary_features_2",
    >>>                                  description="trx_summary_features without the column count_trx")
    >>> # You can also be explicitly specify featuregroup details and what statistics to compute:
    >>> featurestore.create_featuregroup(trx_summary_df1, "trx_summary_features_2_2",
    >>>                                  description="trx_summary_features without the column count_trx",
    >>>                                  featurestore=featurestore.project_featurestore(),featuregroup_version=1,
    >>>                                  jobs=[], descriptive_statistics=False,
    >>>                                  feature_correlation=False, feature_histograms=False, cluster_analysis=False,
    >>>                                  stat_columns=None)
    >>>
    >>> # After you have found the features you need in the featurestore you can materialize the features into a
    >>> # training dataset so that you can train a machine learning model using the features. Just as for featuregroups,
    >>> # it is useful to version and document training datasets, for this reason HopsML supports managed training
    >>> # datasets which enables you to easily version, document and automate the materialization of training datasets.
    >>>
    >>> # First we select the features (and/or labels) that we want
    >>> dataset_df = featurestore.get_features(["pagerank", "triangle_count", "avg_trx", "count_trx", "max_trx",
    >>>                                         "min_trx","balance", "number_of_accounts"],
    >>>                                        featurestore=featurestore.project_featurestore())
    >>> # Now we can create a training dataset from the dataframe with some extended metadata such as schema
    >>> # (automatically inferred).
    >>> # By default when you create a training dataset it will be in "tfrecords" format and statistics will be
    >>> # computed for all features.
    >>> # After the dataset have been created you can view and/or update the metadata about the training dataset
    >>> # from the Hopsworks featurestore UI
    >>> featurestore.create_training_dataset(dataset_df, "AML_dataset")
    >>> # You can override the default configuration if necessary:
    >>> featurestore.create_training_dataset(dataset_df, "TestDataset", description="",
    >>>                                      featurestore=featurestore.project_featurestore(), data_format="csv",
    >>>                                      training_dataset_version=1, jobs=[],
    >>>                                      descriptive_statistics=False, feature_correlation=False,
    >>>                                      feature_histograms=False, cluster_analysis=False, stat_columns=None)
    >>>
    >>> # Once a dataset have been created, its metadata is browsable in the featurestore registry
    >>> # in the Hopsworks UI.
    >>> # If you don't want to create a new training dataset but just overwrite or insert new data into an
    >>> # existing training dataset,
    >>> # you can use the API function 'insert_into_training_dataset'
    >>> featurestore.insert_into_training_dataset(dataset_df, "TestDataset")
    >>> # By default the insert_into_training_dataset will use the project's featurestore, version 1,
    >>> # and update the training dataset statistics, this configuration can be overridden:
    >>> featurestore.insert_into_training_dataset(dataset_df,"TestDataset",
    >>>                                           featurestore=featurestore.project_featurestore(),
    >>>                                           training_dataset_version=1, descriptive_statistics=True,
    >>>                                           feature_correlation=True, feature_histograms=True,
    >>>                                           cluster_analysis=True, stat_columns=None)
    >>>
    >>> # After a managed dataset have been created, it is easy to share it and re-use it for training various models.
    >>> # For example if the dataset have been materialized in tf-records format you can call the method
    >>> # get_training_dataset_path(training_dataset)
    >>> # to get the HDFS path and read it directly in your tensorflow code.
    >>> featurestore.get_training_dataset_path("AML_dataset")
    >>> # By default the library will look for the training dataset in the project's featurestore and use version 1,
    >>> # but this can be overriden if required:
    >>> featurestore.get_training_dataset_path("AML_dataset",  featurestore=featurestore.project_featurestore(),
    >>> training_dataset_version=1)
"""

from hops import util, constants, project
from hops.featurestore_impl.rest import rest_rpc
from hops.featurestore_impl.util import fs_utils
from hops.featurestore_impl import core
from hops.featurestore_impl.exceptions.exceptions import CouldNotConvertDataframe, FeatureVisualizationError
import os
from pathlib import Path
import warnings
import functools


def fs_formatwarning(message, category, filename, lineno, line=None):
    return "{}:{}: {}: {}\n".format(filename, lineno, category.__name__, message)


class StatisticsDeprecationWarning(Warning):
    """A Warning to be raised when methods with deprecated statistics functionality are used."""
    pass


class StatisticsDeprecationError(Exception):
    """An Exception to be raised when methods with deprecated statistics functionality are used."""
    pass


warnings.formatwarning = fs_formatwarning
warnings.simplefilter("always", StatisticsDeprecationWarning)


def _deprecation_warning(fn):
    @functools.wraps(fn)
    def deprecated(*args, **kwargs):
        warnings.warn("Statistics have been deprecated in `hops-util-py`. `{}` will not compute statistics. Please switch to use the new `hsfs` Python client.".format(fn.__name__), StatisticsDeprecationWarning)
        return fn(*args, **kwargs)
    return deprecated


def _deprecation_error(fn):
    @functools.wraps(fn)
    def deprecated(*args, **kwargs):
        raise StatisticsDeprecationError("Statistics have been deprecated in `hops-util-py`. `{}` will not compute statistics. Please switch to use the new `hsfs` Python client.".format(fn.__name__), StatisticsDeprecationWarning)
    return deprecated


def project_featurestore():
    """
    Gets the project's featurestore name (project_featurestore)

    Returns:
        the project's featurestore name

    """
    return fs_utils._do_get_project_featurestore()


def project_training_datasets_sink():
    """
    Gets the project's training datasets sink

    Returns:
        the default training datasets folder in HopsFS for the project

    """
    return fs_utils._do_get_project_training_datasets_sink()


def get_featuregroup(featuregroup, featurestore=None, featuregroup_version=1, dataframe_type="spark",
                     jdbc_args={}, online=False):
    """
    Gets a featuregroup from a featurestore as a spark dataframe

    Example usage:

    >>> #The API will default to version 1 for the feature group and the project's own feature store
    >>> trx_summary_features = featurestore.get_featuregroup("trx_summary_features")
    >>> #You can also explicitly define version and feature store:
    >>> trx_summary_features = featurestore.get_featuregroup("trx_summary_features",
    >>>                                                      featurestore=featurestore.project_featurestore(),
    >>>                                                      featuregroup_version = 1, online=False)

    Args:
        :featuregroup: the featuregroup to get
        :featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        :featuregroup_version: the version of the featuregroup, defaults to 1
        :dataframe_type: the type of the returned dataframe (spark, pandas, python or numpy)
        :jdbc_args: a dict of argument_name -> value with jdbc connection string arguments to be filled in
                    dynamically at runtime for fetching on-demand feature groups
        :online: a boolean flag whether to fetch the online feature group or the offline one (assuming that the
                 feature group has online serving enabled)

    Returns:
        a dataframe with the contents of the featuregroup

    """
    if featurestore is None:
        featurestore = project_featurestore()

    try: # Try with cached metadata
        return core._do_get_featuregroup(featuregroup,
                                         core._get_featurestore_metadata(featurestore, update_cache=False),
                                         featurestore=featurestore, featuregroup_version=featuregroup_version,
                                         dataframe_type = dataframe_type, jdbc_args=jdbc_args, online=online)
    except: # Try again after updating the cache
        return core._do_get_featuregroup(featuregroup,
                                         core._get_featurestore_metadata(featurestore, update_cache=True),
                                         featurestore=featurestore, featuregroup_version=featuregroup_version,
                                         dataframe_type = dataframe_type, jdbc_args=jdbc_args, online=online)


def get_feature(feature, featurestore=None, featuregroup=None, featuregroup_version=1, dataframe_type="spark",
                jdbc_args = {}, online=False):
    """
    Gets a particular feature (column) from a featurestore, if no featuregroup is specified it queries
    hopsworks metastore to see if the feature exists in any of the featuregroups in the featurestore.
    If the user knows which featuregroup contain the feature, it should be specified as it will improve
    performance of the query. Will first try to construct the query from the cached metadata, if that fails,
    it retries after updating the cache

    Example usage:

    >>> #The API will infer the featuregroup and default to version 1 for the feature group with this and the project's
    >>> # own feature store
    >>> max_trx_feature = featurestore.get_feature("max_trx")
    >>> #You can also explicitly define feature group,version and feature store:
    >>> max_trx_feature = featurestore.get_feature("max_trx", featurestore=featurestore.project_featurestore(),
    >>> featuregroup="trx_summary_features", featuregroup_version = 1, online=False)

    Args:
        :feature: the feature name to get
        :featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        :featuregroup: (Optional) the featuregroup where the feature resides
        :featuregroup_version: the version of the featuregroup, defaults to 1
        :dataframe_type: the type of the returned dataframe (spark, pandas, python or numpy)
        :jdbc_args: a dict of argument_name -> value with jdbc connection string arguments to
                    be filled in dynamically at runtime for fetching on-demand feature group in-case the feature
                    belongs to a dynamic feature group
        :online: a boolean flag whether to fetch the online feature or the offline one (assuming that the
                 feature group that the feature is stored in has online serving enabled)
                 (for cached feature groups only)

    Returns:
        A dataframe with the feature

    """
    try:  # try with cached metadata
        return core._do_get_feature(feature, core._get_featurestore_metadata(featurestore, update_cache=False),
                                    featurestore=featurestore, featuregroup=featuregroup,
                                    featuregroup_version=featuregroup_version, dataframe_type=dataframe_type,
                                    jdbc_args=jdbc_args, online=online)
    except:  # Try again after updating cache
        return core._do_get_feature(feature, core._get_featurestore_metadata(featurestore, update_cache=True),
                                    featurestore=featurestore, featuregroup=featuregroup,
                                    featuregroup_version=featuregroup_version, dataframe_type=dataframe_type,
                                    jdbc_args=jdbc_args, online=online)


def get_features(features, featurestore=None, featuregroups_version_dict={}, join_key=None, dataframe_type="spark",
                 jdbc_args = {}, online=False):
    """
    Gets a list of features (columns) from the featurestore. If no featuregroup is specified it will query hopsworks
    metastore to find where the features are stored. It will try to construct the query first from the cached metadata,
    if that fails it will re-try after reloading the cache

    Example usage:

    >>> # The API will default to version 1 for feature groups and the project's feature store
    >>> features = featurestore.get_features(["pagerank", "triangle_count", "avg_trx"],
    >>>                                      featurestore=featurestore.project_featurestore())
    >>> #You can also explicitly define feature group, version, feature store, and join-key:
    >>> features = featurestore.get_features(["pagerank", "triangle_count", "avg_trx"],
    >>>                                     featurestore=featurestore.project_featurestore(),
    >>>                                     featuregroups_version_dict={"trx_graph_summary_features": 1,
    >>>                                     "trx_summary_features": 1}, join_key="cust_id", online=False)

    Args:
        :features: a list of features to get from the featurestore
        :featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        :featuregroups: (Optional) a dict with (fg --> version) for all the featuregroups where the features resides
        :featuregroup_version: the version of the featuregroup, defaults to 1
        :join_key: (Optional) column name to join on
        :dataframe_type: the type of the returned dataframe (spark, pandas, python or numpy)
        :jdbc_args: a dict of featuregroup_version -> dict of argument_name -> value with jdbc connection string
                    arguments to be filled in dynamically at runtime for fetching on-demand feature groups
        :online: a boolean flag whether to fetch the online version of the features (assuming that the
                 feature groups where the features reside have online serving enabled) (for cached feature groups only)

    Returns:
        A dataframe with all the features

    """
    # try with cached metadata
    try:
        return core._do_get_features(features,
                                     core._get_featurestore_metadata(featurestore, update_cache=False),
                                     featurestore=featurestore,
                                     featuregroups_version_dict=featuregroups_version_dict,
                                     join_key=join_key, dataframe_type=dataframe_type, jdbc_args=jdbc_args,
                                     online=online)
        # Try again after updating cache
    except:
        return core._do_get_features(features, core._get_featurestore_metadata(featurestore, update_cache=True),
                                     featurestore=featurestore,
                                     featuregroups_version_dict=featuregroups_version_dict,
                                     join_key=join_key, dataframe_type=dataframe_type, jdbc_args=jdbc_args,
                                     online=online)


def sql(query, featurestore=None, dataframe_type="spark", online=False):
    """
    Executes a generic SQL query on the featurestore

    Example usage:

    >>> # The API will default to the project's feature store
    >>> featurestore.sql("SELECT * FROM trx_graph_summary_features_1 WHERE triangle_count > 5").show(5)
    >>> # You can also explicitly define the feature store
    >>> featurestore.sql("SELECT * FROM trx_graph_summary_features_1 WHERE triangle_count > 5",
    >>>                  featurestore=featurestore.project_featurestore()).show(5)

    Args:
        :query: SQL query
        :featurestore: the featurestore to query, defaults to the project's featurestore
        :dataframe_type: the type of the returned dataframe (spark, pandas, python or numpy)
        :online: boolean flag whether to run the query against the online featurestore (otherwise it will be the offline
                 featurestore)

    Returns:
        A dataframe with the query results

    """
    if featurestore is None:
        featurestore = project_featurestore()
    spark = util._find_spark()
    core._verify_hive_enabled(spark)
    spark.sparkContext.setJobGroup("Running SQL query against feature store",
                                   "Running query: {} on the featurestore {}".format(query, featurestore))
    core._use_featurestore(spark, featurestore)
    result = core._run_and_log_sql(spark, query, online=online, featurestore=featurestore)
    spark.sparkContext.setJobGroup("", "")
    return fs_utils._return_dataframe_type(result, dataframe_type)

@_deprecation_warning
def insert_into_featuregroup(df, featuregroup, featurestore=None, featuregroup_version=1, mode="append", online=False,
                             offline=True):
    """
    **Deprecated**
    Statistics have been deprecated in `hops-util-py`. This method will not compute statistics.
    Please switch to use the new `hsfs` Python client.

    Saves the given dataframe to the specified featuregroup. Defaults to the project-featurestore
    This will append to the featuregroup. To overwrite a featuregroup, create a new version of the featuregroup
    from the UI and append to that table. Statistics will be updated depending on the settings made at creation time of
    the feature group. To update and recompute previously disabled statistics, use the `update_featuregroup_stats`
    method.

    Example usage:

    >>> # The API will default to the project's feature store, featuegroup version 1, and write mode 'append'
    >>> featurestore.insert_into_featuregroup(sampleDf, "trx_graph_summary_features")
    >>> # You can also explicitly define the feature store, the featuregroup version, and the write mode
    >>> # (only append and overwrite are supported)
    >>> featurestore.insert_into_featuregroup(sampleDf, "trx_graph_summary_features",
    >>>                                      featurestore=featurestore.project_featurestore(), featuregroup_version=1,
    >>>                                      mode="append", descriptive_statistics=True, feature_correlation=True,
    >>>                                      feature_histograms=True, cluster_analysis=True,
    >>>                                      stat_columns=None, online=False, offline=True)

    Args:
        :df: the dataframe containing the data to insert into the featuregroup
        :featuregroup: the name of the featuregroup (hive table name)
        :featurestore: the featurestore to save the featuregroup to (hive database)
        :featuregroup_version: the version of the featuregroup (defaults to 1)
        :mode: the write mode, only 'overwrite' and 'append' are supported
        :online: boolean flag whether to insert the data in the online version of the featuregroup
                 (assuming the featuregroup already has online feature serving enabled)
        :offline boolean flag whether to insert the data in the offline version of the featuregroup

    Returns:
        None

    Raises:
        :CouldNotConvertDataframe: in case the provided dataframe could not be converted to a spark dataframe
    """
    try:
        # Try with cached metadata
        core._do_insert_into_featuregroup(df, featuregroup,
                                          core._get_featurestore_metadata(featurestore, update_cache=False),
                                          featurestore=featurestore, featuregroup_version=featuregroup_version,
                                          mode=mode, online=online, offline=offline)
    except:
        # Retry with updated cache
        core._do_insert_into_featuregroup(df, featuregroup,
                                          core._get_featurestore_metadata(featurestore, update_cache=True),
                                          featurestore=featurestore, featuregroup_version=featuregroup_version,
                                          mode=mode, online=online, offline=offline)

@_deprecation_error
def update_featuregroup_stats(featuregroup, featuregroup_version=1, featurestore=None, descriptive_statistics=None,
                              feature_correlation=None, feature_histograms=None, cluster_analysis=None,
                              stat_columns=None, num_bins=None, num_clusters=None, corr_method=None):
    """
    **Deprecated**
    Statistics have been deprecated in `hops-util-py`. Please switch to use the new `hsfs` Python client.

    Updates the statistics settings of a featuregroup and recomputes the specified statistics with spark and then saves
    them to Hopsworks by making a REST call. Leaving a setting set to `None` will keep the previous value.

    Example usage:

    >>> # The API will default to the project's featurestore, featuregroup version 1, and compute all statistics
    >>> # for all columns
    >>> featurestore.update_featuregroup_stats("trx_summary_features")
    >>> # You can also be explicitly specify featuregroup details and what statistics to compute:
    >>> featurestore.update_featuregroup_stats("trx_summary_features", featuregroup_version=1,
    >>>                                       featurestore=featurestore.project_featurestore(),
    >>>                                       descriptive_statistics=True,feature_correlation=True,
    >>>                                       feature_histograms=True, cluster_analysis=True, stat_columns=None)
    >>> # If you only want to compute statistics for certain set of columns and exclude surrogate key-columns for
    >>> # example, you can use the optional argument stat_columns to specify which columns to include:
    >>> featurestore.update_featuregroup_stats("trx_summary_features", featuregroup_version=1,
    >>>                                        featurestore=featurestore.project_featurestore(),
    >>>                                        descriptive_statistics=True, feature_correlation=True,
    >>>                                        feature_histograms=True, cluster_analysis=True,
    >>>                                        stat_columns=['avg_trx', 'count_trx', 'max_trx', 'min_trx'])

    Args:
        :featuregroup: the featuregroup to update the statistics for
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
        None
    """
    pass


def create_on_demand_featuregroup(sql_query, featuregroup, jdbc_connector_name, featurestore=None,
                                  description="", featuregroup_version=1):
    """
    Creates a new on-demand feature group in the feature store by registering SQL and an associated JDBC connector

    Args:
        :sql_query: the SQL query to fetch the on-demand feature group
        :featuregroup: the name of the on-demand feature group
        :jdbc_connector_name: the name of the JDBC connector to apply the SQL query to get the on-demand feature group
        :featurestore: name of the feature store to register the feature group
        :description: description of the feature group
        :featuregroup_version: version of the feature group

    Returns:
        None

    Raises:
        :ValueError: in case required inputs are missing
    """
    if featurestore is None:
        featurestore = project_featurestore()
    if sql_query is None:
        raise ValueError("SQL Query for an on-demand Feature Group cannot be None")
    if jdbc_connector_name is None:
        raise ValueError("Storage Connector for an on-demand Feature Group cannot be None")
    jdbc_connector = get_storage_connector(jdbc_connector_name, featurestore)
    featurestore_metadata = core._get_featurestore_metadata(featurestore, update_cache=False)
    if jdbc_connector.type != featurestore_metadata.settings.jdbc_connector_type:
        raise ValueError("OnDemand Feature groups can only be linked to JDBC Storage Connectors, the provided "
                         "connector is of type: {}".format(jdbc_connector.type))
    featurestore_id = core._get_featurestore_id(featurestore)
    rest_rpc._create_featuregroup_rest(featuregroup, featurestore_id, description, featuregroup_version, [],
                                       None, None, None, None, None,
                                       "onDemandFeaturegroupDTO", sql_query, jdbc_connector.id, False)

    # update metadata cache
    try:
        core._get_featurestore_metadata(featurestore, update_cache=True)
    except:
        pass
    fs_utils._log("Feature group created successfully")


@_deprecation_warning
def create_featuregroup(df, featuregroup, primary_key=[], description="", featurestore=None,
                        featuregroup_version=1, jobs=[],
                        descriptive_statistics=True, feature_correlation=True,
                        feature_histograms=True, cluster_analysis=True, stat_columns=None, num_bins=20,
                        corr_method='pearson', num_clusters=5, partition_by=[], online=False, online_types=None,
                        offline=True):
    """
    **Deprecated**
    Statistics have been deprecated in `hops-util-py`. This method will not compute statistics.
    Please switch to use the new `hsfs` Python client.

    Creates a new cached featuregroup from a dataframe of features (sends the metadata to Hopsworks with a REST call
    to create the Hive table and store the metadata and then inserts the data of the spark dataframe into the newly
    created table).

    The settings for the computation of summary statistics will be saved and applied when new data is inserted. To
    change the settings or recompute the statistics, use the `update_featuregroup_stats` method.

    Example usage:

    >>> # By default the new featuregroup will be created in the project's featurestore and the statistics for the new
    >>> # featuregroup will be computed based on the provided spark dataframe.
    >>> featurestore.create_featuregroup(trx_summary_df1, "trx_summary_features_2",
    >>>                                  description="trx_summary_features without the column count_trx")
    >>> # You can also be explicitly specify featuregroup details and what statistics to compute:
    >>> featurestore.create_featuregroup(trx_summary_df1, "trx_summary_features_2_2",
    >>>                                  description="trx_summary_features without the column count_trx",
    >>>                                  featurestore=featurestore.project_featurestore(),featuregroup_version=1,
    >>>                                  jobs=[], descriptive_statistics=False,
    >>>                                  feature_correlation=False, feature_histograms=False, cluster_analysis=False,
    >>>                                  stat_columns=None, partition_by=[], online=False, offline=True)

    Args:
        :df: the dataframe to create the featuregroup for (used to infer the schema)
        :featuregroup: the name of the new featuregroup
        :primary_key: a list of columns to be used as primary key of the new featuregroup, if not specified,
                      the first column in the dataframe will be used as primary
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
    # Deprecation warning
    if isinstance(primary_key, str):
        print(
            "DeprecationWarning: Primary key of type str is deprecated. With the introduction of composite primary keys"
            " this method expects a list of strings to define the primary key.")
        primary_key = [primary_key]
    # Try with the cache first
    try:
        core._do_create_featuregroup(df, core._get_featurestore_metadata(featurestore, update_cache=False),
                                     featuregroup, primary_key=primary_key, description= description,
                                     featurestore=featurestore, featuregroup_version=featuregroup_version,
                                     jobs=jobs, descriptive_statistics=descriptive_statistics,
                                     feature_correlation=feature_correlation, feature_histograms=feature_histograms,
                                     stat_columns=stat_columns, partition_by=partition_by,
                                     online=online, online_types=online_types, offline=offline)
    # If it fails, update cache and retry
    except:
        core._do_create_featuregroup(df, core._get_featurestore_metadata(featurestore, update_cache=True),
                                     featuregroup, primary_key=primary_key, description= description,
                                     featurestore=featurestore, featuregroup_version=featuregroup_version,
                                     jobs=jobs, descriptive_statistics=descriptive_statistics,
                                     feature_correlation=feature_correlation, feature_histograms=feature_histograms,
                                     stat_columns=stat_columns, partition_by=partition_by,
                                     online=online, online_types=online_types, offline=offline)

    # update metadata cache since we created a new feature group and added new metadata
    try:
        core._get_featurestore_metadata(featurestore, update_cache=True)
    except:
        pass
    fs_utils._log("Feature group created successfully")


def get_featurestore_metadata(featurestore=None, update_cache=False):
    """
    Sends a REST call to Hopsworks to get the list of featuregroups and their features for the given featurestore.

    Example usage:

    >>> # The API will default to the project's feature store
    >>> featurestore.get_featurestore_metadata()
    >>> # You can also explicitly define the feature store
    >>> featurestore.get_featurestore_metadata(featurestore=featurestore.project_featurestore())

    Args:
        :featurestore: the featurestore to query metadata of
        :update_cache: if true the cache is updated

    Returns:
        A list of featuregroups and their metadata

    """
    if featurestore is None:
        featurestore = project_featurestore()
    return core._get_featurestore_metadata(featurestore=featurestore, update_cache=update_cache)


def get_featuregroups(featurestore=None, online=False):
    """
    Gets a list of all featuregroups in a featurestore, uses the cached metadata.

    >>> # List all Feature Groups in a Feature Store
    >>> featurestore.get_featuregroups()
    >>> # By default `get_featuregroups()` will use the project's feature store, but this can also be specified
    >>> # with the optional argument `featurestore`
    >>> featurestore.get_featuregroups(featurestore=featurestore.project_featurestore(), online=False)

    Args:
        :featurestore: the featurestore to list featuregroups for, defaults to the project-featurestore
        :online: flag whether to filter the featuregroups that have online serving enabled

    Returns:
        A list of names of the featuregroups in this featurestore
    """
    if featurestore is None:
        featurestore = project_featurestore()

    # Try with the cache first
    try:
        return fs_utils._do_get_featuregroups(core._get_featurestore_metadata(featurestore, update_cache=False),
                                              online=online)
    # If it fails, update cache
    except:
        return fs_utils._do_get_featuregroups(core._get_featurestore_metadata(featurestore, update_cache=True),
                                              online=online)


def get_features_list(featurestore=None, online=False):
    """
    Gets a list of all features in a featurestore, will use the cached featurestore metadata

    >>> # List all Features in a Feature Store
    >>> featurestore.get_features_list()
    >>> # By default `get_features_list()` will use the project's feature store, but this can also be specified
    >>> # with the optional argument `featurestore`
    >>> featurestore.get_features_list(featurestore=featurestore.project_featurestore())

    Args:
        :featurestore: the featurestore to list features for, defaults to the project-featurestore
        :online: flag whether to filter the features that have online serving enabled

    Returns:
        A list of names of the features in this featurestore
    """
    if featurestore is None:
        featurestore = project_featurestore()
    try:
        return fs_utils._do_get_features_list(core._get_featurestore_metadata(featurestore, update_cache=False),
                                              online=online)
    except:
        return fs_utils._do_get_features_list(core._get_featurestore_metadata(featurestore, update_cache=True),
                                              online=online)


def get_featuregroup_features_list(featuregroup, version=None, featurestore=None):
    """
    Gets a list of the names of the features in a featuregroup.

    Args:
        :featuregroup: Name of the featuregroup to get feature names for.
        :version: Version of the featuregroup to use. Defaults to the latest version.
        :featurestore: The featurestore to list features for. Defaults to project-featurestore.

    Returns:
        A list of names of the features in this featuregroup.
    """
    if featurestore is None:
        featurestore = project_featurestore()
    try:
        if version is None:
            version = fs_utils._do_get_latest_featuregroup_version(
                featuregroup, core._get_featurestore_metadata(featurestore, update_cache=False))
        return fs_utils._do_get_featuregroup_features_list(
            featuregroup, version, core._get_featurestore_metadata(featurestore, update_cache=False))
    except:
        if version is None:
            version = fs_utils._do_get_latest_featuregroup_version(
                featuregroup, core._get_featurestore_metadata(featurestore, update_cache=True))
        return fs_utils._do_get_featuregroup_features_list(
            featuregroup, version, core._get_featurestore_metadata(featurestore, update_cache=True))


def get_training_datasets(featurestore=None):
    """
    Gets a list of all training datasets in a featurestore, will use the cached metadata

    >>> # List all Training Datasets in a Feature Store
    >>> featurestore.get_training_datasets()
    >>> # By default `get_training_datasets()` will use the project's feature store, but this can also be specified
    >>> # with the optional argument featurestore
    >>> featurestore.get_training_datasets(featurestore=featurestore.project_featurestore())

    Args:
        :featurestore: the featurestore to list training datasets for, defaults to the project-featurestore

    Returns:
        A list of names of the training datasets in this featurestore
    """
    if featurestore is None:
        featurestore = project_featurestore()
    try:
        return core._do_get_training_datasets(core._get_featurestore_metadata(featurestore, update_cache=False))
    except:
        return core._do_get_training_datasets(core._get_featurestore_metadata(featurestore, update_cache=True))


def get_project_featurestores():
    """
    Gets all featurestores for the current project

    Example usage:

    >>> # Get list of featurestores accessible by the current project example
    >>> featurestore.get_project_featurestores()

    Returns:
        A list of all featurestores that the project have access to

    """
    featurestores_json = rest_rpc._get_featurestores()
    featurestoreNames = list(map(lambda fsj: fsj[constants.REST_CONFIG.JSON_FEATURESTORE_NAME], featurestores_json))
    return featurestoreNames


def get_dataframe_tf_record_schema(spark_df, fixed=True):
    """
    Infers the tf-record schema from a spark dataframe
    Note: this method is just for convenience, it should work in 99% of cases but it is not guaranteed,
    if spark or tensorflow introduces new datatypes this will break. The user can allways fallback to encoding the
    tf-example-schema manually.

    Args:
        :spark_df: the spark dataframe to infer the tensorflow example record from
        :fixed: boolean flag indicating whether array columns should be treated with fixed size or variable size

    Returns:
        a dict with the tensorflow example
    """
    return fs_utils._get_dataframe_tf_record_schema_json(spark_df, fixed=fixed)[0]


def get_training_dataset_tf_record_schema(training_dataset, training_dataset_version=1, featurestore=None):
    """
    Gets the tf record schema for a training dataset that is stored in tfrecords format

    Example usage:

    >>> # get tf record schema for a tfrecords dataset
    >>> featurestore.get_training_dataset_tf_record_schema("team_position_prediction", training_dataset_version=1,
    >>>                                                    featurestore = featurestore.project_featurestore())

    Args:
        :training_dataset: the training dataset to get the tfrecords schema for
        :training_dataset_version: the version of the training dataset
        :featurestore: the feature store where the training dataset resides

    Returns:
        the tf records schema

    """
    if featurestore is None:
        featurestore = project_featurestore()
    try:
        return core._do_get_training_dataset_tf_record_schema(training_dataset,
                                                              core._get_featurestore_metadata(featurestore,
                                                                                              update_cache=False),
                                                              training_dataset_version=training_dataset_version,
                                                              featurestore=featurestore)
    except:
        return core._do_get_training_dataset_tf_record_schema(training_dataset,
                                                              core._get_featurestore_metadata(featurestore,
                                                                                              update_cache=True),
                                                              training_dataset_version=training_dataset_version,
                                                              featurestore=featurestore)


def get_training_dataset(training_dataset, featurestore=None, training_dataset_version=1, dataframe_type="spark"):
    """
    Reads a training dataset into a spark dataframe, will first look for the training dataset using the cached metadata
    of the featurestore, if it fails it will reload the metadata and try again.

    Example usage:
    >>> featurestore.get_training_dataset("team_position_prediction_csv").show(5)

    Args:
        :training_dataset: the name of the training dataset to read
        :featurestore: the featurestore where the training dataset resides
        :training_dataset_version: the version of the training dataset
        :dataframe_type: the type of the returned dataframe (spark, pandas, python or numpy)

    Returns:
        A dataframe with the given training dataset data
    """
    if featurestore is None:
        featurestore = project_featurestore()
    try:
        return core._do_get_training_dataset(training_dataset,
                                             core._get_featurestore_metadata(featurestore, update_cache=False),
                                             training_dataset_version=training_dataset_version,
                                             dataframe_type=dataframe_type,
                                             featurestore=featurestore)
    except:
        return core._do_get_training_dataset(training_dataset,
                                             core._get_featurestore_metadata(featurestore, update_cache=True),
                                             training_dataset_version=training_dataset_version,
                                             dataframe_type=dataframe_type)


def get_training_dataset_features_list(training_dataset, version=None, featurestore=None):
    """
    Gets a list of the names of the features in a training dataset.

    Args:
        :training_dataset: Name of the training dataset to get feature names for.
        :version: Version of the training dataset to use. Defaults to the latest version.
        :featurestore: The featurestore to look for the dataset for. Defaults to project-featurestore.

    Returns:
        A list of names of the features in this training dataset.
    """
    if featurestore is None:
        featurestore = project_featurestore()
    try:
        if version is None:
            version = fs_utils._do_get_latest_training_dataset_version(
                training_dataset, core._get_featurestore_metadata(featurestore, update_cache=False))
        return fs_utils._do_get_training_dataset_features_list(
            training_dataset, version, core._get_featurestore_metadata(featurestore, update_cache=False))
    except:
        if version is None:
            version = fs_utils._do_get_latest_training_dataset_version(
                training_dataset, core._get_featurestore_metadata(featurestore, update_cache=True))
        return fs_utils._do_get_training_dataset_features_list(
            training_dataset, version, core._get_featurestore_metadata(featurestore, update_cache=True))


@_deprecation_warning
def create_training_dataset(df, training_dataset, description="", featurestore=None,
                            data_format="tfrecords", write_mode="overwrite", training_dataset_version=1,
                            jobs=[], descriptive_statistics=True, feature_correlation=True,
                            feature_histograms=True, cluster_analysis=True, stat_columns=None, num_bins=20,
                            corr_method='pearson', num_clusters=5, petastorm_args={}, fixed=True, sink=None, path=None):
    """
    **Deprecated**
    Statistics have been deprecated in `hops-util-py`. This method will not compute statistics.
    Please switch to use the new `hsfs` Python client.

    Creates a new training dataset from a dataframe, saves metadata about the training dataset to the database
    and saves the materialized dataset on hdfs

    Example usage:

    >>> featurestore.create_training_dataset(dataset_df, "AML_dataset")
    >>> # You can override the default configuration if necessary:
    >>> featurestore.create_training_dataset(dataset_df, "TestDataset", description="",
    >>>                                      featurestore=featurestore.project_featurestore(), data_format="csv",
    >>>                                      training_dataset_version=1,
    >>>                                      descriptive_statistics=False, feature_correlation=False,
    >>>                                      feature_histograms=False, cluster_analysis=False, stat_columns=None,
    >>>                                      sink = None, path=None)

    Args:
        :df: the dataframe to create the training dataset from
        :training_dataset: the name of the training dataset
        :description: a description of the training dataset
        :featurestore: the featurestore that the training dataset is linked to
        :data_format: the format of the materialized training dataset
        :training_dataset_version: the version of the training dataset (defaults to 1)
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
        :sink: name of storage connector to store the training dataset
        :jobs: list of Hopsworks jobs linked to the training dataset
        :path: path to complement the sink storage connector with, e.g if the storage connector points to an
               S3 bucket, this path can be used to define a sub-directory inside the bucket to place the training
               dataset.

    Returns:
        None
    """
    if featurestore is None:
        featurestore = project_featurestore()
    if sink is None:
        sink = project_training_datasets_sink()
    if util.get_job_name() is not None:
        jobs.append(util.get_job_name())
    storage_connector = core._do_get_storage_connector(sink, featurestore)
    core._do_create_training_dataset(df, training_dataset, description, featurestore, data_format, write_mode,
                                     training_dataset_version, jobs, descriptive_statistics,
                                     feature_correlation, feature_histograms, stat_columns,
                                     petastorm_args, fixed, storage_connector, path)


def get_storage_connectors(featurestore = None):
    """
    Retrieves the names of all storage connectors in the feature store

    Example usage:

    >>> featurestore.get_storage_connectors()
    >>> # By default the query will be for the project's feature store but you can also explicitly specify the
    >>> # featurestore:
    >>> featurestore.get_storage_connector(featurestore=featurestore.project_featurestore())

    Args:
        :featurestore: the featurestore to query (default's to project's feature store)

    Returns:
        the storage connector with the given name
    """
    if featurestore is None:
        featurestore = project_featurestore()
    try:
        return core._do_get_storage_connectors(core._get_featurestore_metadata(featurestore, update_cache=False))
    except:
        return core._do_get_storage_connectors(core._get_featurestore_metadata(featurestore, update_cache=True))


def get_storage_connector(storage_connector_name, featurestore = None):
    """
    Looks up a storage connector by name

    Example usage:

    >>> featurestore.get_storage_connector("demo_featurestore_admin000_Training_Datasets")
    >>> # By default the query will be for the project's feature store but you can also explicitly specify the
    >>> # featurestore:
    >>> featurestore.get_storage_connector("demo_featurestore_admin000_Training_Datasets",
    >>>                                    featurestore=featurestore.project_featurestore())

    Args:
        :storage_connector_name: the name of the storage connector
        :featurestore: the featurestore to query (default's to project's feature store)

    Returns:
        the storage connector with the given name
    """
    if featurestore is None:
        featurestore = project_featurestore()
    return core._do_get_storage_connector(storage_connector_name, featurestore)


@_deprecation_warning
def insert_into_training_dataset(
        df, training_dataset, featurestore=None, training_dataset_version=1,
        descriptive_statistics=True, feature_correlation=True,
        feature_histograms=True, cluster_analysis=True, stat_columns=None, num_bins=20, corr_method='pearson',
        num_clusters=5, write_mode="overwrite", ):
    """
    **Deprecated**
    Statistics have been deprecated in `hops-util-py`. This method will not compute statistics.
    Please switch to use the new `hsfs` Python client.

    Inserts the data in a training dataset from a spark dataframe (append or overwrite)

    Example usage:

    >>> featurestore.insert_into_training_dataset(dataset_df, "TestDataset")
    >>> # By default the insert_into_training_dataset will use the project's featurestore, version 1,
    >>> # and update the training dataset statistics, this configuration can be overridden:
    >>> featurestore.insert_into_training_dataset(dataset_df,"TestDataset",
    >>>                                           featurestore=featurestore.project_featurestore(),
    >>>                                           training_dataset_version=1,descriptive_statistics=True,
    >>>                                           feature_correlation=True, feature_histograms=True,
    >>>                                           cluster_analysis=True, stat_columns=None)

    Args:
        :df: the dataframe to write
        :training_dataset: the name of the training dataset
        :featurestore: the featurestore that the training dataset is linked to
        :training_dataset_version: the version of the training dataset (defaults to 1)
        :descriptive_statistics: a boolean flag whether to compute descriptive statistics (min,max,mean etc)
                                for the featuregroup
        :feature_correlation: a boolean flag whether to compute a feature correlation matrix for the numeric columns
                              in the featuregroup
        :feature_histograms: a boolean flag whether to compute histograms for the numeric columns in the featuregroup
        :cluster_analysis: a boolean flag whether to compute cluster analysis for the numeric columns
                          in the featuregroup
        :stat_columns: a list of columns to compute statistics for (defaults to all columns that are numeric)
        :num_bins: number of bins to use for computing histograms
        :num_clusters: number of clusters to use for cluster analysis
        :corr_method: the method to compute feature correlation with (pearson or spearman)
        :write_mode: spark write mode ('append' or 'overwrite'). Note: append is not supported for tfrecords datasets.

    Returns:
        None

    """
    if featurestore is None:
        featurestore = project_featurestore()
    try:
        core._do_insert_into_training_dataset(df, training_dataset,
                                              core._get_featurestore_metadata(featurestore,
                                                                              update_cache=False),
                                              featurestore,
                                              training_dataset_version=training_dataset_version,
                                              write_mode=write_mode)
        fs_utils._log("Insertion into training dataset was successful")
    except:
        core._do_insert_into_training_dataset(df, training_dataset,
                                              core._get_featurestore_metadata(featurestore,
                                                                              update_cache=True),
                                              featurestore,
                                              training_dataset_version=training_dataset_version,
                                              write_mode=write_mode)
        fs_utils._log("Insertion into training dataset was successful")


def get_training_dataset_path(training_dataset, featurestore=None, training_dataset_version=1):
    """
    Gets the HDFS path to a training dataset with a specific name and version in a featurestore

    Example usage:

    >>> featurestore.get_training_dataset_path("AML_dataset")
    >>> # By default the library will look for the training dataset in the project's featurestore and use version 1,
    >>> # but this can be overriden if required:
    >>> featurestore.get_training_dataset_path("AML_dataset",  featurestore=featurestore.project_featurestore(),
    >>>                                        training_dataset_version=1)

    Args:
        :training_dataset: name of the training dataset
        :featurestore: featurestore that the training dataset is linked to
        :training_dataset_version: version of the training dataset

    Returns:
        The HDFS path to the training dataset
    """
    if featurestore is None:
        featurestore = project_featurestore()
    try:
        return core._do_get_training_dataset_path(training_dataset,
                                                  core._get_featurestore_metadata(featurestore,
                                                                                  update_cache=False),
                                                  training_dataset_version=training_dataset_version)
    except:
        return core._do_get_training_dataset_path(training_dataset,
                                                  core._get_featurestore_metadata(featurestore,
                                                                                  update_cache=True),
                                                  training_dataset_version=training_dataset_version)


def get_latest_training_dataset_version(training_dataset, featurestore=None):
    """
    Utility method to get the latest version of a particular training dataset

    Example usage:

    >>> featurestore.get_latest_training_dataset_version("team_position_prediction")

    Args:
        :training_dataset: the training dataset to get the latest version of
        :featurestore: the featurestore where the training dataset resides

    Returns:
        the latest version of the training dataset in the feature store
    """
    if featurestore is None:
        featurestore = project_featurestore()

    return fs_utils._do_get_latest_training_dataset_version(training_dataset,
                                                            core._get_featurestore_metadata(featurestore,
                                                                                            update_cache=True))


def get_latest_featuregroup_version(featuregroup, featurestore=None):
    """
    Utility method to get the latest version of a particular featuregroup

    Example usage:

    >>> featurestore.get_latest_featuregroup_version("teams_features_spanish")

    Args:
        :featuregroup: the featuregroup to get the latest version of
        :featurestore: the featurestore where the featuregroup resides

    Returns:
        the latest version of the featuregroup in the feature store
    """
    if featurestore is None:
        featurestore = project_featurestore()

    return fs_utils._do_get_latest_featuregroup_version(featuregroup,
                                                        core._get_featurestore_metadata(featurestore,
                                                                                        update_cache=True))


@_deprecation_error
def update_training_dataset_stats(training_dataset, training_dataset_version=1, featurestore=None,
                                  descriptive_statistics=True,
                                  feature_correlation=True, feature_histograms=True, cluster_analysis=True,
                                  stat_columns=None, num_bins=20,
                                  num_clusters=5, corr_method='pearson'):
    """
    **Deprecated**
    Statistics have been deprecated in `hops-util-py`. Please switch to use the new `hsfs` Python client.

    Updates the statistics of a featuregroup by computing the statistics with spark and then saving it to Hopsworks by
    making a REST call.

    Example usage:

    >>> # The API will default to the project's featurestore, training dataset version 1, and compute all statistics
    >>> # for all columns
    >>> featurestore.update_training_dataset_stats("teams_prediction")
    >>> # You can also be explicitly specify featuregroup details and what statistics to compute:
    >>> featurestore.update_training_dataset_stats("teams_prediction", training_dataset_version=1,
    >>>                                            featurestore=featurestore.project_featurestore(),
    >>>                                            descriptive_statistics=True,feature_correlation=True,
    >>>                                            feature_histograms=True, cluster_analysis=True, stat_columns=None)
    >>> # If you only want to compute statistics for certain set of columns and exclude surrogate key-columns
    >>> # for example, you can use the optional argument stat_columns to specify which columns to include:
    >>> featurestore.update_training_dataset_stats("teams_prediction", training_dataset_version=1,
    >>>                                            featurestore=featurestore.project_featurestore(),
    >>>                                            descriptive_statistics=True, feature_correlation=True,
    >>>                                            feature_histograms=True, cluster_analysis=True,
    >>>                                            stat_columns=['avg_trx', 'count_trx', 'max_trx', 'min_trx'])

    Args:
        :training_dataset: the training dataset to update the statistics for
        :training_dataset_version: the version of the training dataset (defaults to 1)
        :featurestore: the featurestore where the training dataset resides (defaults to the project's featurestore)
        :descriptive_statistics: a boolean flag whether to compute descriptive statistics (min,max,mean etc) for
                                 the featuregroup
        :feature_correlation: a boolean flag whether to compute a feature correlation matrix for the numeric columns
                              in the featuregroup
        :feature_histograms: a boolean flag whether to compute histograms for the numeric columns in the featuregroup
        :cluster_analysis: a boolean flag whether to compute cluster analysis for the numeric columns in
                           the featuregroup
        :stat_columns: a list of columns to compute statistics for (defaults to all columns that are numeric)
        :num_bins: number of bins to use for computing histograms
        :num_clusters: the number of clusters to use in clustering analysis (k-means)
        :corr_method: the method to compute feature correlation with (pearson or spearman)

    Returns:
        None
    """
    pass


def get_featuregroup_partitions(featuregroup, featurestore=None, featuregroup_version=1, dataframe_type="spark"):
    """
    Gets the partitions of a featuregroup

    Example usage:

    >>> partitions = featurestore.get_featuregroup_partitions("trx_summary_features")
    >>> #You can also explicitly define version, featurestore and type of the returned dataframe:
    >>> featurestore.get_featuregroup_partitions("trx_summary_features",
    >>>                                          featurestore=featurestore.project_featurestore(),
    >>>                                          featuregroup_version = 1,
    >>>                                          dataframe_type="spark")

     Args:
        :featuregroup: the featuregroup to get partitions for
        :featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        :featuregroup_version: the version of the featuregroup, defaults to 1
        :dataframe_type: the type of the returned dataframe (spark, pandas, python or numpy)

     Returns:
        a dataframe with the partitions of the featuregroup
     """
    if featurestore is None:
        featurestore = project_featurestore()
    try:
        # Try with cached metadata
        return core._do_get_featuregroup_partitions(featuregroup,
                                                    core._get_featurestore_metadata(featurestore, update_cache=False),
                                                    featurestore, featuregroup_version, dataframe_type)
    except:
        # Retry with updated cache
        return core._do_get_featuregroup_partitions(featuregroup,
                                                    core._get_featurestore_metadata(featurestore, update_cache=True),
                                                    featurestore, featuregroup_version, dataframe_type)

@_deprecation_error
def visualize_featuregroup_distributions(featuregroup_name, featurestore=None, featuregroup_version=1, figsize=None,
                                         color='lightblue', log=False, align="center", plot=True):
    """
    **Deprecated**
    Statistics have been deprecated in `hops-util-py`. Please switch to use the new `hsfs` Python client.

    Visualizes the feature distributions (if they have been computed) for a featuregroup in the featurestore

    Example usage:

    >>> featurestore.visualize_featuregroup_distributions("trx_summary_features")
    >>> # You can also explicitly define version, featurestore and plotting options
    >>> featurestore.visualize_featuregroup_distributions("trx_summary_features",
    >>>                                                  featurestore=featurestore.project_featurestore(),
    >>>                                                  featuregroup_version = 1,
    >>>                                                  color="lightblue",
    >>>                                                  figsize=None,
    >>>                                                  log=False,
    >>>                                                  align="center",
    >>>                                                  plot=True)

    Args:
        :featuregroup_name: the name of the featuregroup
        :featurestore: the featurestore where the featuregroup resides
        :featuregroup_version: the version of the featuregroup
        :figsize: size of the figure. If None, gets automatically set
        :color: the color of the histograms
        :log: whether to use log-scaling on the y-axis or not
        :align: how to align the bars, defaults to center.
        :plot: if set to True it will plot the image and return None, if set to False it will not plot it
               but rather return the figure

    Returns:
        if the 'plot' flag is set to True it will plot the image and return None, if the 'plot' flag is set to False
        it will not plot it but rather return the figure

    Raises:
        :FeatureVisualizationError: if there was an error visualizing the feature distributions
    """
    pass


@_deprecation_error
def visualize_featuregroup_correlations(featuregroup_name, featurestore=None, featuregroup_version=1, figsize=(16,12),
                                        cmap="coolwarm", annot=True, fmt=".2f", linewidths=.05, plot=True):
    """
    **Deprecated**
    Statistics have been deprecated in `hops-util-py`. Please switch to use the new `hsfs` Python client.

    Visualizes the feature correlations (if they have been computed) for a featuregroup in the featurestore

    Example usage:

    >>> featurestore.visualize_featuregroup_correlations("trx_summary_features")
    >>> # You can also explicitly define version, featurestore and plotting options
    >>> featurestore.visualize_featuregroup_correlations("trx_summary_features",
    >>>                                                  featurestore=featurestore.project_featurestore(),
    >>>                                                  featuregroup_version = 1,
    >>>                                                  cmap="coolwarm",
    >>>                                                  figsize=(16,12),
    >>>                                                  annot=True,
    >>>                                                  fmt=".2f",
    >>>                                                  linewidths=.05
    >>>                                                  plot=True)

    Args:
        :featuregroup_name: the name of the featuregroup
        :featurestore: the featurestore where the featuregroup resides
        :featuregroup_version: the version of the featuregroup
        :figsize: the size of the figure
        :cmap: the color map
        :annot: whether to annotate the heatmap
        :fmt: how to format the annotations
        :linewidths: line width in the plot
        :plot: if set to True it will plot the image and return None, if set to False it will not plot it
               but rather return the figure

    Returns:
        if the 'plot' flag is set to True it will plot the image and return None, if the 'plot' flag is set to False
        it will not plot it but rather return the figure

    Raises:
        :FeatureVisualizationError: if there was an error visualizing the feature correlations
    """
    pass


@_deprecation_error
def visualize_featuregroup_clusters(featuregroup_name, featurestore=None, featuregroup_version=1, figsize=(16,12),
                                    plot=True):
    """
    **Deprecated**
    Statistics have been deprecated in `hops-util-py`. Please switch to use the new `hsfs` Python client.

    Visualizes the feature clusters (if they have been computed) for a featuregroup in the featurestore

    Example usage:

    >>> featurestore.visualize_featuregroup_clusters("trx_summary_features")
    >>> # You can also explicitly define version, featurestore and plotting options
    >>> featurestore.visualize_featuregroup_clusters("trx_summary_features",
    >>>                                                  featurestore=featurestore.project_featurestore(),
    >>>                                                  featuregroup_version = 1,
    >>>                                                  figsize=(16,12),
    >>>                                                  plot=True)

    Args:
        :featuregroup_name: the name of the featuregroup
        :featurestore: the featurestore where the featuregroup resides
        :featuregroup_version: the version of the featuregroup
        :figsize: the size of the figure
        :plot: if set to True it will plot the image and return None, if set to False it will not plot it
               but rather return the figure

    Returns:
        if the 'plot' flag is set to True it will plot the image and return None, if the 'plot' flag is set to False
        it will not plot it but rather return the figure

    Raises:
        :FeatureVisualizationError: if there was an error visualizing the feature clusters
    """
    pass


@_deprecation_error
def visualize_featuregroup_descriptive_stats(featuregroup_name, featurestore=None, featuregroup_version=1):
    """
    **Deprecated**
    Statistics have been deprecated in `hops-util-py`. Please switch to use the new `hsfs` Python client.

    Visualizes the descriptive stats (if they have been computed) for a featuregroup in the featurestore

    Example usage:

    >>> featurestore.visualize_featuregroup_descriptive_stats("trx_summary_features")
    >>> # You can also explicitly define version, featurestore and plotting options
    >>> featurestore.visualize_featuregroup_descriptive_stats("trx_summary_features",
    >>>                                                  featurestore=featurestore.project_featurestore(),
    >>>                                                  featuregroup_version = 1)

    Args:
        :featuregroup_name: the name of the featuregroup
        :featurestore: the featurestore where the featuregroup resides
        :featuregroup_version: the version of the featuregroup

    Returns:
        A pandas dataframe with the descriptive statistics

    Raises:
        :FeatureVisualizationError: if there was an error in fetching the descriptive statistics
    """
    pass


@_deprecation_error
def visualize_training_dataset_distributions(training_dataset_name, featurestore=None, training_dataset_version=1,
                                             figsize=(16, 12), color='lightblue', log=False, align="center", plot=True):
    """
    **Deprecated**
    Statistics have been deprecated in `hops-util-py`. Please switch to use the new `hsfs` Python client.

    Visualizes the feature distributions (if they have been computed) for a training dataset in the featurestore

    Example usage:

    >>> featurestore.visualize_training_dataset_distributions("AML_dataset")
    >>> # You can also explicitly define version, featurestore and plotting options
    >>> featurestore.visualize_training_dataset_distributions("AML_dataset",
    >>>                                                  featurestore=featurestore.project_featurestore(),
    >>>                                                  training_dataset_version = 1,
    >>>                                                  color="lightblue",
    >>>                                                  figsize=(16,12),
    >>>                                                  log=False,
    >>>                                                  align="center",
    >>>                                                  plot=True)

    Args:
        :training_dataset_name: the name of the training dataset
        :featurestore: the featurestore where the training dataset resides
        :training_dataset_version: the version of the training dataset
        :figsize: size of the figure
        :figsize: the size of the figure
        :color: the color of the histograms
        :log: whether to use log-scaling on the y-axis or not
        :align: how to align the bars, defaults to center.
        :plot: if set to True it will plot the image and return None, if set to False it will not plot it
               but rather return the figure

    Returns:
        if the 'plot' flag is set to True it will plot the image and return None, if the 'plot' flag is set to False
        it will not plot it but rather return the figure

    Raises:
        :FeatureVisualizationError: if there was an error visualizing the feature distributions
    """
    pass


@_deprecation_error
def visualize_training_dataset_correlations(training_dataset_name, featurestore=None, training_dataset_version=1,
                                            figsize=(16,12), cmap="coolwarm", annot=True, fmt=".2f",
                                            linewidths=.05, plot=True):
    """
    **Deprecated**
    Statistics have been deprecated in `hops-util-py`. Please switch to use the new `hsfs` Python client.

    Visualizes the feature distributions (if they have been computed) for a training dataset in the featurestore

    Example usage:

    >>> featurestore.visualize_training_dataset_correlations("AML_dataset")
    >>> # You can also explicitly define version, featurestore and plotting options
    >>> featurestore.visualize_training_dataset_correlations("AML_dataset",
    >>>                                                  featurestore=featurestore.project_featurestore(),
    >>>                                                  training_dataset_version = 1,
    >>>                                                  cmap="coolwarm",
    >>>                                                  figsize=(16,12),
    >>>                                                  annot=True,
    >>>                                                  fmt=".2f",
    >>>                                                  linewidths=.05
    >>>                                                  plot=True)

    Args:
        :training_dataset_name: the name of the training dataset
        :featurestore: the featurestore where the training dataset resides
        :training_dataset_version: the version of the training dataset
        :figsize: the size of the figure
        :cmap: the color map
        :annot: whether to annotate the heatmap
        :fmt: how to format the annotations
        :linewidths: line width in the plot
        :plot: if set to True it will plot the image and return None, if set to False it will not plot it
               but rather return the figure

    Returns:
        if the 'plot' flag is set to True it will plot the image and return None, if the 'plot' flag is set to False
        it will not plot it but rather return the figure

    Raises:
        :FeatureVisualizationError: if there was an error visualizing the feature correlations
    """
    pass


@_deprecation_error
def visualize_training_dataset_clusters(training_dataset_name, featurestore=None, training_dataset_version=1,
                                        figsize=(16,12), plot=True):
    """
    **Deprecated**
    Statistics have been deprecated in `hops-util-py`. Please switch to use the new `hsfs` Python client.

    Visualizes the feature clusters (if they have been computed) for a training dataset in the featurestore

    Example usage:

    >>> featurestore.visualize_training_dataset_clusters("AML_dataset")
    >>> # You can also explicitly define version, featurestore and plotting options
    >>> featurestore.visualize_training_dataset_clusters("AML_dataset",
    >>>                                                  featurestore=featurestore.project_featurestore(),
    >>>                                                  training_dataset_version = 1,
    >>>                                                  figsize=(16,12),
    >>>                                                  plot=True)

    Args:
        :training_dataset_name: the name of the training dataset
        :featurestore: the featurestore where the training dataset resides
        :training_dataset_version: the version of the training dataset
        :figsize: the size of the figure
        :plot: if set to True it will plot the image and return None, if set to False it will not plot it
               but rather return the figure

    Returns:
        if the 'plot' flag is set to True it will plot the image and return None, if the 'plot' flag is set to False
        it will not plot it but rather return the figure

    Raises:
        :FeatureVisualizationError: if there was an error visualizing the feature clusters
    """
    pass


@_deprecation_error
def visualize_training_dataset_descriptive_stats(training_dataset_name, featurestore=None, training_dataset_version=1):
    """
    **Deprecated**
    Statistics have been deprecated in `hops-util-py`. Please switch to use the new `hsfs` Python client.

    Visualizes the descriptive stats (if they have been computed) for a training dataset in the featurestore

    Example usage:

    >>> featurestore.visualize_training_dataset_descriptive_stats("AML_dataset")
    >>> # You can also explicitly define version and featurestore
    >>> featurestore.visualize_training_dataset_descriptive_stats("AML_dataset",
    >>>                                                  featurestore=featurestore.project_featurestore(),
    >>>                                                  training_dataset_version = 1)

    Args:
        :training_dataset_name: the name of the training dataset
        :featurestore: the featurestore where the training dataset resides
        :training_dataset_version: the version of the training dataset

    Returns:
        A pandas dataframe with the descriptive statistics

    Raises:
        :FeatureVisualizationError: if there was an error in fetching the descriptive statistics
    """
    pass


@_deprecation_error
def get_featuregroup_statistics(featuregroup_name, featurestore=None, featuregroup_version=1):
    """
    **Deprecated**
    Statistics have been deprecated in `hops-util-py`. Please switch to use the new `hsfs` Python client.

    Gets the computed statistics (if any) of a featuregroup

    Example usage:

    >>> stats = featurestore.get_featuregroup_statistics("trx_summary_features")
    >>> # You can also explicitly define version and featurestore
    >>> stats = featurestore.get_featuregroup_statistics("trx_summary_features",
    >>>                                                  featurestore=featurestore.project_featurestore(),
    >>>                                                  featuregroup_version = 1)

    Args:
        :featuregroup_name: the name of the featuregroup
        :featurestore: the featurestore where the featuregroup resides
        :featuregroup_version: the version of the featuregroup

    Returns:
          A Statistics Object
    """
    pass


@_deprecation_error
def get_training_dataset_statistics(training_dataset_name, featurestore=None, training_dataset_version=1):
    """
    **Deprecated**
    Statistics have been deprecated in `hops-util-py`. Please switch to use the new `hsfs` Python client.

    Gets the computed statistics (if any) of a training dataset

    Example usage:

    >>> stats = featurestore.get_training_dataset_statistics("AML_dataset")
    >>> # You can also explicitly define version and featurestore
    >>> stats = featurestore.get_training_dataset_statistics("AML_dataset",
    >>>                                                      featurestore=featurestore.project_featurestore(),
    >>>                                                      training_dataset_version = 1)

    Args:
        :training_dataset_name: the name of the training dataset
        :featurestore: the featurestore where the training dataset resides
        :training_dataset_version: the version of the training dataset

    Returns:
          A Statistics Object
    """
    pass

def connect(host, project_name, port = 443, region_name = constants.AWS.DEFAULT_REGION,
            secrets_store = 'parameterstore', api_key_file=None, cert_folder="hops",
            hostname_verification=True, trust_store_path=None):
    """
    Connects to a feature store from a remote environment such as Amazon SageMaker

    Example usage:

    >>> featurestore.connect("hops.site", "my_feature_store")

    Args:
        :host: the hostname of the Hopsworks cluster
        :project_name: the name of the project hosting the feature store to be used
        :port: the REST port of the Hopsworks cluster
        :region_name: The name of the AWS region in which the required secrets are stored
        :secrets_store: The secrets storage to be used. Either secretsmanager, parameterstore or local
        :api_key_file: path to a file containing an API key. For secrets_store=local only.
        :cert_folder: the folder on dbfs where to store the HopsFS certificates
        :hostname_verification: whether or not to verify Hopsworks' certificate - default True
        :trust_store_path: path on dbfs containing the Hopsworks certificates 

    Returns:
        None
    """
    dbfs_folder = "/dbfs/" + cert_folder
    os.environ[constants.ENV_VARIABLES.REST_ENDPOINT_END_VAR] = "https://" + host + ':' + str(port)
    os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_NAME_ENV_VAR] = project_name
    os.environ[constants.ENV_VARIABLES.REGION_NAME_ENV_VAR] = region_name
    os.environ[constants.ENV_VARIABLES.API_KEY_ENV_VAR] = util.get_secret(secrets_store, 'api-key', api_key_file)
    os.environ[constants.ENV_VARIABLES.REQUESTS_VERIFY_ENV_VAR] = str(hostname_verification).lower()

    if not trust_store_path is None:
        os.environ[constants.ENV_VARIABLES.DOMAIN_CA_TRUSTSTORE_PEM_ENV_VAR] = "/dbfs/" + trust_store_path

    project_info = project.get_project_info(project_name)
    project_id = str(project_info['projectId'])
    os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_ID_ENV_VAR] = project_id

    Path(dbfs_folder).mkdir(parents=True, exist_ok=True)

    get_credential(project_id, dbfs_folder)

def setup_databricks(host, project_name, cert_folder="hops",
                     port = 443, region_name = constants.AWS.DEFAULT_REGION,
                     secrets_store = 'parameterstore', api_key_file=None,
                     hostname_verification=True, trust_store_path=None):
    """
    Set up the HopsFS and Hive connector on a Databricks cluster

    Example usage:

    >>> featurestore.setup_databricks("hops.site", "my_feature_store")

    Args:
        :host: the hostname of the Hopsworks cluster
        :project_name: the name of the project hosting the feature store to be used
        :certs_folder: the folder on dbfs in which to store the Hopsworks certificates and the libraries to be installed when the cluster restart
        :port: the REST port of the Hopsworks cluster
        :region_name: The name of the AWS region in which the required secrets are stored
        :secrets_store: The secrets storage to be used. Either secretsmanager or parameterstore.
        :api_key_file: path to a file containing an API key. For secrets_store=local only.
        :hostname_verification: whether or not to verify Hopsworks' certificate - default True
        :trust_store_path: path on dbfs containing the Hopsworks certificates 

    Returns:
        None
    """
    
    dbfs_folder = "/dbfs/" + cert_folder
    os.environ[constants.ENV_VARIABLES.REST_ENDPOINT_END_VAR] = "https://" + host + ':' + str(port)
    os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_NAME_ENV_VAR] = project_name
    os.environ[constants.ENV_VARIABLES.REGION_NAME_ENV_VAR] = region_name
    os.environ[constants.ENV_VARIABLES.API_KEY_ENV_VAR] = util.get_secret(secrets_store, 'api-key', api_key_file)
    os.environ[constants.ENV_VARIABLES.REQUESTS_VERIFY_ENV_VAR] = str(hostname_verification).lower()

    if not trust_store_path is None:
        os.environ[constants.ENV_VARIABLES.DOMAIN_CA_TRUSTSTORE_PEM_ENV_VAR] = "/dbfs/" + trust_store_path

    project_info = project.get_project_info(project_name)
    project_id = str(project_info['projectId'])
    os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_ID_ENV_VAR] = project_id

    Path(dbfs_folder + "/scripts").mkdir(parents=True, exist_ok=True)

    get_credential(project_id, dbfs_folder)

    get_clients(project_id, dbfs_folder)
    
    write_init_script(dbfs_folder)

    hive_endpoint = rest_rpc._get_featurestores()[0]['hiveEndpoint']
    private_host = hive_endpoint.split(":")[0]

    print_instructions(cert_folder, dbfs_folder, private_host)
    
def get_credential(project_id, dbfs_folder):
    """
    get the credential and save them in the dbfs folder

    Args:
        :project_id: the id of the project for which we want to get credentials
        :dbfs_folder: the folder in which to save the credentials
    """
    credentials = rest_rpc._get_credentials(project_id)
    util.write_b64_cert_to_bytes(str(credentials['kStore']), path=os.path.join(dbfs_folder, 'keyStore.jks'))
    util.write_b64_cert_to_bytes(str(credentials['tStore']), path=os.path.join(dbfs_folder, 'trustStore.jks'))
    with open(os.path.join(dbfs_folder, 'material_passwd'), 'w') as f:
        f.write(str(credentials['password']))
        
def get_clients(project_id, dbfs_folder):
    """
    get the libraries and save them in the dbfs folder

    Args:
        :project_id: the id of the project for which we want to get the clients
        :dbfs_folder: the folder in which to save the libraries
    """
    client = rest_rpc._get_client(project_id)
    with open(os.path.join(dbfs_folder, 'client.tar.gz'), 'wb') as f:
        for chunk in client:
            f.write(chunk)    

def write_init_script(dbfs_folder):
    """
    write the init script

    Args:
        :dbfs_folder: the folder in which to save the script
    """
    initScript="""
        #!/bin/sh

        tar -xvf PATH/client.tar.gz -C /tmp
        tar -xvf /tmp/client/apache-hive-*-bin.tar.gz -C /tmp
        mv /tmp/apache-hive-*-bin /tmp/apache-hive-bin
        chmod -R +xr /tmp/apache-hive-bin
        cp /tmp/client/hopsfs-client*.jar /databricks/jars/
    """
    initScript=initScript.replace("PATH", dbfs_folder)
    with open(os.path.join(dbfs_folder, 'scripts/initScript.sh'), 'w') as f:
        f.write(initScript)

def print_instructions(cert_folder, dbfs_folder, host):
    """
    print the instructions to set up the hopsfs hive connection on databricks

    Args:
        :cert_folder: the path in dbfs of the folder in which the credention were saved
        :dbfs_folder: the folder in which the credential were saved
        :host: the host of the hive metastore    
    """

    instructions="""
    In the advanced options of your databricks cluster configuration 
    add the following path to Init Scripts: dbfs:/{0}/scripts/initScript.sh

    add the following to the Spark Config:
    spark.hadoop.fs.hopsfs.impl io.hops.hopsfs.client.HopsFileSystem
    spark.hadoop.hops.ipc.server.ssl.enabled true
    spark.hadoop.hops.ssl.hostname.verifier ALLOW_ALL
    spark.hadoop.hops.rpc.socket.factory.class.default io.hops.hadoop.shaded.org.apache.hadoop.net.HopsSSLSocketFactory
    spark.hadoop.client.rpc.ssl.enabled.protocol TLSv1.2
    spark.hadoop.hops.ssl.keystores.passwd.name {1}/material_passwd
    spark.hadoop.hops.ssl.keystore.name {1}/keyStore.jks
    spark.hadoop.hops.ssl.trustore.name {1}/trustStore.jks
    spark.sql.hive.metastore.jars /tmp/apache-hive-bin/lib/*
    spark.hadoop.hive.metastore.uris thrift://{2}:9083

    Then save and restart the cluster.
    """.format(cert_folder, dbfs_folder, host)

    print(instructions)

@_deprecation_warning
def sync_hive_table_with_featurestore(featuregroup, description="", featurestore=None,
                                      featuregroup_version=1, jobs=[], feature_corr_data = None,
                                      featuregroup_desc_stats_data = None, features_histogram_data = None,
                                      cluster_analysis_data = None):
    """
    **Deprecated**
    Statistics have been deprecated in `hops-util-py`. This method will not compute statistics.
    Please switch to use the new `hsfs` Python client.

    Synchronizes an existing Hive table with a Feature Store.

    Example usage:
    
    >>> # Save Hive Table
    >>> sample_df.write.mode("overwrite").saveAsTable("hive_fs_sync_example_1")
    >>> # Synchronize with Feature Store
    >>> featurestore.sync_hive_table_with_featurestore("hive_fs_sync_example", featuregroup_version=1)

    Args:
        :featuregroup: name of the featuregroup to synchronize with the hive table.
                       The hive table should have a naming scheme of featuregroup_version
        :description: description of the feature group
        :featurestore: the feature store where the hive table is stored
        :featuregroup_version: version of the feature group
        :jobs: jobs to compute this feature group (optional)
        :feature_corr_data: correlation statistics (optional)
        :featuregroup_desc_stats_data: descriptive statistics (optional)
        :features_histogram_data: histogram statistics (optional)
        :cluster_analysis_data: cluster analysis (optional)

    Returns:
        None
    """
    if featurestore is None:
        featurestore = project_featurestore()

    fs_utils._log("Synchronizing Hive Table: {} with Feature Store: {}".format(featuregroup, featurestore))

    try: # Try with cached metadata
        core._sync_hive_table_with_featurestore(
            featuregroup, core._get_featurestore_metadata(featurestore, update_cache=False),
            description=description, featurestore=featurestore, featuregroup_version=featuregroup_version,
            jobs=jobs)
    except: # Try again after updating the cache
        core._sync_hive_table_with_featurestore(
            featuregroup, core._get_featurestore_metadata(featurestore, update_cache=True),
            description=description, featurestore=featurestore, featuregroup_version=featuregroup_version,
            jobs=jobs)

    # update metadata cache
    try:
        core._get_featurestore_metadata(featurestore, update_cache=True)
    except:
        pass

    fs_utils._log("Hive Table: {} was successfully synchronized with Feature Store: {}".format(featuregroup,
                                                                                               featurestore))


def import_featuregroup_redshift(storage_connector, query, featuregroup, primary_key=[], description="",
                                 featurestore=None, featuregroup_version=1, jobs=[], descriptive_statistics=True,
                                 feature_correlation=True, feature_histograms=True, cluster_analysis=True,
                                 stat_columns=None, num_bins=20, corr_method='pearson', num_clusters=5,
                                 partition_by=[], online=False, online_types=None, offline=True):
    """
    Imports an external dataset of features into a feature group in Hopsworks.
    This function will read the dataset using spark and a configured JDBC storage connector for Redshift
    and then writes the data to Hopsworks Feature Store (Hive) and registers its metadata.

    Example usage:

    >>> featurestore.import_featuregroup_redshift(my_jdbc_connector_name, sql_query, featuregroup_name)
    >>> # You can also be explicitly specify featuregroup metadata and what statistics to compute:
    >>> featurestore.import_featuregroup_redshift(my_jdbc_connector_name, sql_query, featuregroup_name,
    >>>                                  primary_key=["id"],
    >>>                                  description="trx_summary_features without the column count_trx",
    >>>                                  featurestore=featurestore.project_featurestore(), featuregroup_version=1,
    >>>                                  jobs=[], descriptive_statistics=False,
    >>>                                  feature_correlation=False, feature_histograms=False, cluster_analysis=False,
    >>>                                  stat_columns=None, partition_by=[])

    Args:
        :storage_connector: the storage connector used to connect to the external storage
        :query: the queury extracting data from Redshift
        :featuregroup: name of the featuregroup to import the dataset into the featurestore
        :primary_key: a list of columns to be used as primary key of the new featuregroup, if not specified,
                      the first column in the dataframe will be used as primary key
        :description: metadata description of the feature group to import
        :featurestore: name of the featurestore database to import the feature group into
        :featuregroup_version: version of the feature group
        :jobs: list of Hopsworks jobs linked to the feature group (optional)
        :descriptive_statistics: a boolean flag whether to compute descriptive statistics (min,max,mean etc) for the
                                 featuregroup
        :feature_correlation: a boolean flag whether to compute a feature correlation matrix for the numeric columns in
                              the featuregroup
        :feature_histograms: a boolean flag whether to compute histograms for the numeric columns in the featuregroup
        :cluster_analysis: a boolean flag whether to compute cluster analysis for the numeric columns in the
                           featuregroup
        :stat_columns: a list of columns to compute statistics for (defaults to all columns that are numeric)
        :num_bins: number of bins to use for computing histograms
        :corr_method: the method to compute feature correlation with (pearson or spearman)
        :num_clusters: the number of clusters to use for cluster analysis
        :partition_by: a list of columns to partition_by, defaults to the empty list
        :online: boolean flag, if this is set to true, a MySQL table for online feature data will be created in
                 addition to the Hive table for offline feature data
        :online_types: a dict with feature_name --> online_type, if a feature is present in this dict,
                            the online_type will be taken from the dict rather than inferred from the spark dataframe.
        :offline boolean flag whether to insert the data in the offline version of the featuregroup

    Returns:
        None
    """
    # Deprecation warning
    if isinstance(primary_key, str):
        print(
            "DeprecationWarning: Primary key of type str is deprecated. With the introduction of composite primary keys"
            " this method expects a list of strings to define the primary key.")
        primary_key = [primary_key]
    if featurestore is None:
        featurestore = project_featurestore()

    try: # try with metadata cache
        spark_df = core._do_get_redshift_featuregroup(storage_connector, query,
                                            core._get_featurestore_metadata(featurestore, update_cache=False),
                                            featurestore=featurestore)
        create_featuregroup(spark_df, featuregroup, primary_key=primary_key, description=description,
                            featurestore=featurestore, featuregroup_version=featuregroup_version, jobs=jobs,
                            descriptive_statistics=descriptive_statistics, feature_correlation=feature_correlation,
                            feature_histograms=feature_histograms, cluster_analysis=cluster_analysis,
                            stat_columns=stat_columns, num_bins=num_bins, corr_method=corr_method,
                            num_clusters=num_clusters, partition_by=partition_by, online=online,
                            online_type=online_types, offline=offline)
    except: # retry with updated metadata
        spark_df = core._do_get_redshift_featuregroup(storage_connector, query,
                                                      core._get_featurestore_metadata(featurestore, update_cache=True),
                                                      featurestore=featurestore)
        create_featuregroup(spark_df, featuregroup, primary_key=primary_key, description=description,
                            featurestore=featurestore, featuregroup_version=featuregroup_version, jobs=jobs,
                            descriptive_statistics=descriptive_statistics, feature_correlation=feature_correlation,
                            feature_histograms=feature_histograms, cluster_analysis=cluster_analysis,
                            stat_columns=stat_columns, num_bins=num_bins, corr_method=corr_method,
                            num_clusters=num_clusters, partition_by=partition_by, online=online,
                            online_types=online_types, offline=offline)

    fs_utils._log("Feature group imported successfully")


def import_featuregroup_s3(storage_connector, path, featuregroup, primary_key=[], description="", featurestore=None,
                           featuregroup_version=1, jobs=[],
                           descriptive_statistics=True, feature_correlation=True,
                           feature_histograms=True, cluster_analysis=True, stat_columns=None, num_bins=20,
                           corr_method='pearson', num_clusters=5, partition_by=[], data_format="parquet", online=False,
                           online_types=None, offline=True):
    """
    Imports an external dataset of features into a feature group in Hopsworks.
    This function will read the dataset using spark and a configured storage connector (e.g to an S3 bucket)
    and then writes the data to Hopsworks Feature Store (Hive) and registers its metadata.

    Example usage:

    >>> featurestore.import_featuregroup_s3(my_s3_connector_name, s3_path, featuregroup_name,
    >>>                                  data_format=s3_bucket_data_format)
    >>> # You can also be explicitly specify featuregroup metadata and what statistics to compute:
    >>> featurestore.import_featuregroup_s3(my_s3_connector_name, s3_path, featuregroup_name, primary_key=["id"],
    >>>                                  description="trx_summary_features without the column count_trx",
    >>>                                  featurestore=featurestore.project_featurestore(),featuregroup_version=1,
    >>>                                  jobs=[], descriptive_statistics=False,
    >>>                                  feature_correlation=False, feature_histograms=False, cluster_analysis=False,
    >>>                                  stat_columns=None, partition_by=[], data_format="parquet", online=False,
    >>>                                  online_types=None, offline=True)

    Args:
        :storage_connector: the storage connector used to connect to the external storage
        :path: the path to read from the external storage
        :featuregroup: name of the featuregroup to import the dataset into the featurestore
        :primary_key: a list of columns to be used as primary key of the new featuregroup, if not specified,
                      the first column in the dataframe will be used as primary key
        :description: metadata description of the feature group to import
        :featurestore: name of the featurestore database to import the feature group into
        :featuregroup_version: version of the feature group
        :jobs: list of Hopsworks jobs linked to the feature group (optional)
        :descriptive_statistics: a boolean flag whether to compute descriptive statistics (min,max,mean etc) for the
                                 featuregroup
        :feature_correlation: a boolean flag whether to compute a feature correlation matrix for the numeric columns in
                              the featuregroup
        :feature_histograms: a boolean flag whether to compute histograms for the numeric columns in the featuregroup
        :cluster_analysis: a boolean flag whether to compute cluster analysis for the numeric columns in the
                           featuregroup
        :stat_columns: a list of columns to compute statistics for (defaults to all columns that are numeric)
        :num_bins: number of bins to use for computing histograms
        :corr_method: the method to compute feature correlation with (pearson or spearman)
        :num_clusters: the number of clusters to use for cluster analysis
        :partition_by: a list of columns to partition_by, defaults to the empty list
        :data_format: the format of the external dataset to read
        :online: boolean flag, if this is set to true, a MySQL table for online feature data will be created in
                 addition to the Hive table for offline feature data
        :online_types: a dict with feature_name --> online_type, if a feature is present in this dict,
                            the online_type will be taken from the dict rather than inferred from the spark dataframe.
        :offline boolean flag whether to insert the data in the offline version of the featuregroup

    Returns:
        None
    """
    # Deprecation warning
    if isinstance(primary_key, str):
        print(
            "DeprecationWarning: Primary key of type str is deprecated. With the introduction of composite primary keys"
            " this method expects a list of strings to define the primary key.")
        primary_key = [primary_key]
    # update metadata cache
    if featurestore is None:
        featurestore = project_featurestore()

    try: # try with metadata cache
        spark_df = core._do_get_s3_featuregroup(storage_connector, path,
                                                core._get_featurestore_metadata(featurestore, update_cache=False),
                                                featurestore=featurestore, data_format=data_format)
        create_featuregroup(spark_df, featuregroup, primary_key=primary_key, description=description,
                            featurestore=featurestore, featuregroup_version=featuregroup_version, jobs=jobs,
                            descriptive_statistics=descriptive_statistics, feature_correlation=feature_correlation,
                            feature_histograms=feature_histograms, cluster_analysis=cluster_analysis,
                            stat_columns=stat_columns, num_bins=num_bins, corr_method=corr_method,
                            num_clusters=num_clusters, partition_by=partition_by, online=online,
                            online_types=online_types, offline=offline)
    except: # retry with updated metadata
        spark_df = core._do_get_s3_featuregroup(storage_connector, path,
                                                core._get_featurestore_metadata(featurestore, update_cache=True),
                                                featurestore=featurestore, data_format=data_format)
        create_featuregroup(spark_df, featuregroup, primary_key=primary_key, description=description,
                            featurestore=featurestore, featuregroup_version=featuregroup_version, jobs=jobs,
                            descriptive_statistics=descriptive_statistics, feature_correlation=feature_correlation,
                            feature_histograms=feature_histograms, cluster_analysis=cluster_analysis,
                            stat_columns=stat_columns, num_bins=num_bins, corr_method=corr_method,
                            num_clusters=num_clusters, partition_by=partition_by, online=online,
                            online_types=online_types, offline=offline)

    fs_utils._log("Feature group imported successfully")


def get_online_featurestore_connector(featurestore=None):
    """
    Gets a JDBC connector for the online feature store

    Args:
        :featurestore: the feature store name

    Returns:
        a DTO object of the JDBC connector for the online feature store
    """
    if featurestore is None:
        featurestore = project_featurestore()
    try: # try with metadata cache
        return core._do_get_online_featurestore_connector(featurestore,
                                                   core._get_featurestore_metadata(featurestore, update_cache=False))
    except: # retry with updated metadata
        return core._do_get_online_featurestore_connector(featurestore,
                                                   core._get_featurestore_metadata(featurestore, update_cache=True))


def enable_featuregroup_online(featuregroup_name, featuregroup_version=1, featurestore=None, online_types=None):
    """
    Enables online feature serving for a feature group

    Example usage:

    >>> # The API will default to the project's feature store
    >>> featurestore.enable_featurergroup_online(featuregroup_name)
    >>> # You can also explicitly override the default arguments:
    >>> featurestore.enable_featurergroup_online(featuregroup_name, featuregroup_version=1, featurestore=featurestore,
    >>>                                          online_types=False)

    Args:
        :featuregroup_name: name of the featuregroup
        :featuregroup_version: version of the featuregroup
        :featurestore: the featurestore that the featuregroup belongs to
        :online_types: a dict with feature_name --> online_type, if a feature is present in this dict,
                            the online_type will be taken from the dict rather than inferred from the spark dataframe.

    Returns:
        None
    """
    if featurestore is None:
        featurestore = fs_utils._do_get_project_featurestore()

    try: # try with metadata cache
        core._do_enable_featuregroup_online(featuregroup_name, featuregroup_version,
                                                      core._get_featurestore_metadata(featurestore, update_cache=False),
                                                      featurestore=featurestore, online_types=online_types)
    except: # retry with updated metadata
        core._do_enable_featuregroup_online(featuregroup_name, featuregroup_version,
                                            core._get_featurestore_metadata(featurestore, update_cache=True),
                                            featurestore=featurestore, online_types=online_types)

    fs_utils._log("Online Feature Serving enabled successfully for featuregroup: {}".format(featuregroup_name))


def disable_featuregroup_online(featuregroup_name, featuregroup_version=1, featurestore=None):
    """
    Enables online feature serving for a feature group

    Example usage:

    >>> # The API will default to the project's feature store
    >>> featurestore.disable_featurergroup_online(featuregroup_name)
    >>> # You can also explicitly override the default arguments:
    >>> featurestore.disable_featurergroup_online(featuregroup_name, featuregroup_version=1, featurestore=featurestore)

    Args:
        :featuregroup_name: name of the featuregroup
        :featuregroup_version: version of the featuregroup
        :featurestore: the featurestore that the featuregroup belongs to

    Returns:
        None
    """
    if featurestore is None:
        featurestore = fs_utils._do_get_project_featurestore()

    try: # try with metadata cache
        core._do_disable_featuregroup_online(featuregroup_name, featuregroup_version,
                                            core._get_featurestore_metadata(featurestore, update_cache=False))
    except: # retry with updated metadata
        core._do_disable_featuregroup_online(featuregroup_name, featuregroup_version,
                                            core._get_featurestore_metadata(featurestore, update_cache=True))

    fs_utils._log("Online Feature Serving disabled successfully for featuregroup: {}".format(featuregroup_name))


def set_featuregroup_tag(name, tag, value=None, version=1, featurestore=None):
    """
    Attach tag to a feature group

    Example usage:

    >>> # The API will default to the project's feature store
    >>> featurestore.set_featuregroup_tag(featuregroup_name, "SPORT")
    >>> # You can also explicitly override the default arguments:
    >>> featurestore.set_featuregroup_tag(featuregroup_name, "SPORT", value="Football", version=1, featurestore=featurestore)

    Args:
        :name: name of the featuregroup
        :tag: name of the tag
        :value: value of the tag
        :version: version of the featuregroup
        :featurestore: the featurestore that the featuregroup belongs to

    Returns:
        None
    """

    core._do_add_tag(name, tag, value, featurestore, version, constants.REST_CONFIG.HOPSWORKS_FEATUREGROUPS_RESOURCE)


def get_featuregroup_tags(name, version=1, featurestore=None):
    """
    Get all tags attached to a feature group

    Example usage:

    >>> # The API will default to the project's feature store
    >>> tags = featurestore.get_featuregroup_tags(featuregroup_name)
    >>> # You can also explicitly override the default arguments:
    >>> tags = featurestore.get_featuregroup_tags(featuregroup_name, version=1, featurestore=featurestore)

    Args:
        :name: name of the featuregroup
        :version: version of the featuregroup
        :featurestore: the featurestore that the featuregroup belongs to

    Returns:
        The tags dictionary attached to the featuregroup
    """

    return core._do_get_tags(name, featurestore, version, constants.REST_CONFIG.HOPSWORKS_FEATUREGROUPS_RESOURCE)


def remove_featuregroup_tag(name, tag, version=1, featurestore=None):
    """
    Removes all tags attached to a feature group

    Example usage:

    >>> # The API will default to the project's feature store
    >>> featurestore.remove_featuregroup_tags(featuregroup_name)
    >>> # You can also explicitly override the default arguments:
    >>> featurestore.remove_featuregroup_tags(featuregroup_name, version=1, featurestore=featurestore)

    Args:
        :name: name of the featuregroup
        :tag: name of the tag to remove from the featuregroup
        :version: version of the featuregroup
        :featurestore: the featurestore that the featuregroup belongs to

    Returns:
        None
    """

    core._do_remove_tag(name, tag, featurestore, version, constants.REST_CONFIG.HOPSWORKS_FEATUREGROUPS_RESOURCE)


def set_training_dataset_tag(name, tag, value=None, version=1, featurestore=None):
    """
    Attach tag to a training dataset

    Example usage:

    >>> # The API will default to the project's feature store
    >>> featurestore.set_training_dataset_tag(training_dataset_name, "SPORT")
    >>> # You can also explicitly override the default arguments:
    >>> featurestore.set_training_dataset_tag(training_dataset_name, "SPORT", value="Football", version=1, featurestore=featurestore)

    Args:
        :name: name of the training dataset
        :tag: name of the tag
        :value: value of the tag
        :version: version of the training dataset
        :featurestore: the featurestore that the training dataset belongs to

    Returns:
        None
    """

    core._do_add_tag(name, tag, value, featurestore, version, constants.REST_CONFIG.HOPSWORKS_TRAININGDATASETS_RESOURCE)


def get_training_dataset_tags(name, version=1, featurestore=None):
    """
    Get tags attached to a training dataset

    Example usage:

    >>> # The API will default to the project's feature store
    >>> tags = featurestore.get_training_dataset_tags(training_dataset_name)
    >>> # You can also explicitly override the default arguments:
    >>> tags = featurestore.get_training_dataset_tags(training_dataset_name, version=1, featurestore=featurestore)

    Args:
        :name: name of the training dataset
        :training_dataset_version: version of the training dataset
        :featurestore: the featurestore that the training dataset belongs to

    Returns:
        The tags dictionary attached to the training dataset
    """

    return core._do_get_tags(name, featurestore, version, constants.REST_CONFIG.HOPSWORKS_TRAININGDATASETS_RESOURCE)


def remove_training_dataset_tag(name, tag, version=1, featurestore=None):
    """
    Removes all tags attached to a training dataset

    Example usage:

    >>> # The API will default to the project's feature store
    >>> featurestore.remove_training_dataset_tags(training_dataset_name)
    >>> # You can also explicitly override the default arguments:
    >>> featurestore.remove_training_dataset_tags(training_dataset_name, version=1, featurestore=featurestore)

    Args:
        :name: name of the training dataset
        :tag: name of the tag to remove from the training dataset
        :version: version of the training dataset
        :featurestore: the featurestore that the training dataset belongs to

    Returns:
        None
    """

    core._do_remove_tag(name, tag, featurestore, version, constants.REST_CONFIG.HOPSWORKS_TRAININGDATASETS_RESOURCE)


def get_tags():
    """
    Get tags that can be attached to a featuregroup or training dataset

    Example usage:

    >>> tags = featurestore.get_tags()

    Returns:
        List of tags
    """

    return core._do_get_fs_tags()

