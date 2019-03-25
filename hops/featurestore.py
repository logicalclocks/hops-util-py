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
    >>> trx_summary_features = featurestore.get_featuregroup("trx_summary_features", featurestore=featurestore.project_featurestore(), featuregroup_version = 1)
    >>>
    >>> # Get single feature example
    >>> #The API will infer the featuregroup and default to version 1 for the feature group with this and the project's own feature store
    >>> max_trx_feature = featurestore.get_feature("max_trx")
    >>> #You can also explicitly define feature group,version and feature store:
    >>> max_trx_feature = featurestore.get_feature("max_trx", featurestore=featurestore.project_featurestore(), featuregroup="trx_summary_features", featuregroup_version = 1)
    >>> # When you want to get features from different feature groups the API will infer how to join the features together
    >>>
    >>> # Get list of features example
    >>> # The API will default to version 1 for feature groups and the project's feature store
    >>> features = featurestore.get_features(["pagerank", "triangle_count", "avg_trx"], featurestore=featurestore.project_featurestore())
    >>> #You can also explicitly define feature group, version, feature store, and join-key:
    >>> features = featurestore.get_features(["pagerank", "triangle_count", "avg_trx"], featurestore=featurestore.project_featurestore(), featuregroups_version_dict={"trx_graph_summary_features": 1, "trx_summary_features": 1}, join_key="cust_id")
    >>>
    >>> # Run SQL query against feature store example
    >>> # The API will default to the project's feature store
    >>> featurestore.sql("SELECT * FROM trx_graph_summary_features_1 WHERE triangle_count > 5").show(5)
    >>> # You can also explicitly define the feature store
    >>> featurestore.sql("SELECT * FROM trx_graph_summary_features_1 WHERE triangle_count > 5", featurestore=featurestore.project_featurestore()).show(5)
    >>>
    >>> # Insert into featuregroup example
    >>> # The API will default to the project's feature store, featuegroup version 1, and write mode 'append'
    >>> featurestore.insert_into_featuregroup(sampleDf, "trx_graph_summary_features")
    >>> # You can also explicitly define the feature store, the featuregroup version, and the write mode (only append and overwrite are supported)
    >>> featurestore.insert_into_featuregroup(sampleDf, "trx_graph_summary_features", featurestore=featurestore.project_featurestore(), featuregroup_version=1, mode="append", descriptive_statistics=True, feature_correlation=True, feature_histograms=True, cluster_analysis=True, stat_columns=None)
    >>>
    >>> # Get featurestore metadata example
    >>> # The API will default to the project's feature store
    >>> featurestore.get_featurestore_metadata()
    >>> # You can also explicitly define the feature store
    >>> featurestore.get_featurestore_metadata(featurestore=featurestore.project_featurestore())
    >>>
    >>> # List all Feature Groups in a Feature Store
    >>> featurestore.get_featuregroups()
    >>> # By default `get_featuregroups()` will use the project's feature store, but this can also be specified with the optional argument `featurestore`
    >>> featurestore.get_featuregroups(featurestore=featurestore.project_featurestore())
    >>>
    >>> # List all Training Datasets in a Feature Store
    >>> featurestore.get_training_datasets()
    >>> # By default `get_training_datasets()` will use the project's feature store, but this can also be specified with the optional argument featurestore
    >>> featurestore.get_training_datasets(featurestore=featurestore.project_featurestore())
    >>>
    >>> # Get list of featurestores accessible by the current project example
    >>> featurestore.get_project_featurestores()
    >>> # By default `get_featurestore_metadata` will use the project's feature store, but this can also be specified with the optional argument featurestore
    >>> featurestore.get_featurestore_metadata(featurestore=featurestore.project_featurestore())
    >>>
    >>> # Compute featuergroup statistics (feature correlation, descriptive stats, feature distributions etc) with Spark that will show up in the Featurestore Registry in Hopsworks
    >>> # The API will default to the project's featurestore, featuregroup version 1, and compute all statistics for all columns
    >>> featurestore.update_featuregroup_stats("trx_summary_features")
    >>> # You can also be explicitly specify featuregroup details and what statistics to compute:
    >>> featurestore.update_featuregroup_stats("trx_summary_features", featuregroup_version=1, featurestore=featurestore.project_featurestore(), descriptive_statistics=True,feature_correlation=True, feature_histograms=True, cluster_analysis=True, stat_columns=None)
    >>> # If you only want to compute statistics for certain set of columns and exclude surrogate key-columns for example, you can use the optional argument stat_columns to specify which columns to include:
    >>> featurestore.update_featuregroup_stats("trx_summary_features", featuregroup_version=1, featurestore=featurestore.project_featurestore(), descriptive_statistics=True, feature_correlation=True, feature_histograms=True, cluster_analysis=True, stat_columns=['avg_trx', 'count_trx', 'max_trx', 'min_trx'])
    >>>
    >>> # Create featuregroup from an existing dataframe
    >>> # In most cases it is recommended that featuregroups are created in the UI on Hopsworks and that care is taken in documenting the featuregroup.
    >>> # However, sometimes it is practical to create a featuregroup directly from a spark dataframe and fill in the metadata about the featuregroup later in the UI.
    >>> # This can be done through the create_featuregroup API function
    >>>
    >>> # By default the new featuregroup will be created in the project's featurestore and the statistics for the new featuregroup will be computed based on the provided spark dataframe.
    >>> featurestore.create_featuregroup(trx_summary_df1, "trx_summary_features_2", description="trx_summary_features without the column count_trx")
    >>> # You can also be explicitly specify featuregroup details and what statistics to compute:
    >>> featurestore.create_featuregroup(trx_summary_df1, "trx_summary_features_2_2", description="trx_summary_features without the column count_trx",featurestore=featurestore.project_featurestore(),featuregroup_version=1, job_name=None, dependencies=[], descriptive_statistics=False, feature_correlation=False, feature_histograms=False, cluster_analysis=False, stat_columns=None)
    >>>
    >>> # After you have found the features you need in the featurestore you can materialize the features into a training dataset
    >>> # so that you can train a machine learning model using the features. Just as for featuregroups,
    >>> # it is useful to version and document training datasets, for this reason HopsML supports managed training datasets which enables
    >>> # you to easily version, document and automate the materialization of training datasets.
    >>>
    >>> # First we select the features (and/or labels) that we want
    >>> dataset_df = featurestore.get_features(["pagerank", "triangle_count", "avg_trx", "count_trx", "max_trx", "min_trx","balance", "number_of_accounts"],featurestore=featurestore.project_featurestore())
    >>> # Now we can create a training dataset from the dataframe with some extended metadata such as schema (automatically inferred).
    >>> # By default when you create a training dataset it will be in "tfrecords" format and statistics will be computed for all features.
    >>> # After the dataset have been created you can view and/or update the metadata about the training dataset from the Hopsworks featurestore UI
    >>> featurestore.create_training_dataset(dataset_df, "AML_dataset")
    >>> # You can override the default configuration if necessary:
    >>> featurestore.create_training_dataset(dataset_df, "TestDataset", description="", featurestore=featurestore.project_featurestore(), data_format="csv", training_dataset_version=1, job_name=None, dependencies=[], descriptive_statistics=False, feature_correlation=False, feature_histograms=False, cluster_analysis=False, stat_columns=None)
    >>>
    >>> # Once a dataset have been created, its metadata is browsable in the featurestore registry in the Hopsworks UI.
    >>> # If you don't want to create a new training dataset but just overwrite or insert new data into an existing training dataset,
    >>> # you can use the API function 'insert_into_training_dataset'
    >>> featurestore.insert_into_training_dataset(dataset_df, "TestDataset")
    >>> # By default the insert_into_training_dataset will use the project's featurestore, version 1,
    >>> # and update the training dataset statistics, this configuration can be overridden:
    >>> featurestore.insert_into_training_dataset(dataset_df,"TestDataset", featurestore=featurestore.project_featurestore(), training_dataset_version=1, descriptive_statistics=True, feature_correlation=True, feature_histograms=True, cluster_analysis=True, stat_columns=None)
    >>>
    >>> # After a managed dataset have been created, it is easy to share it and re-use it for training various models.
    >>> # For example if the dataset have been materialized in tf-records format you can call the method get_training_dataset_path(training_dataset)
    >>> # to get the HDFS path and read it directly in your tensorflow code.
    >>> featurestore.get_training_dataset_path("AML_dataset")
    >>> # By default the library will look for the training dataset in the project's featurestore and use version 1, but this can be overriden if required:
    >>> featurestore.get_training_dataset_path("AML_dataset",  featurestore=featurestore.project_featurestore(), training_dataset_version=1)
"""

from hops import util
from hops import hdfs
from hops import tls
import pydoop.hdfs as pydoop
from hops import constants
import math
from pyspark.sql.utils import AnalysisException
import json
from pyspark.mllib.stat import Statistics
import pandas as pd
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import PCA
from pyspark.ml.linalg import Vectors
from pyspark.ml.clustering import KMeans
import re
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.rdd import RDD
from pyspark.sql import SQLContext
from tempfile import TemporaryFile

# for backwards compatibility
try:
    import h5py
except:
    pass

# for backwards compatibility
try:
    import tensorflow as tf
except:
    pass


def project_featurestore():
    """
    Gets the project's featurestore name (project_featurestore)

    Returns:
        the project's featurestore name

    """
    project_name = hdfs.project_name()
    featurestore_name = project_name.lower() + constants.FEATURE_STORE.FEATURESTORE_SUFFIX
    return featurestore_name

def _log(x):
    """
    Generic log function (in case logging is changed from stdout later)

    Args:
        :x: the argument to log

    Returns:
        None
    """
    print(x)


def _run_and_log_sql(spark, sql_str):
    """
    Runs and logs an SQL query with sparkSQL

    Args:
        :spark: the spark session
        :sql_str: the query to run

    Returns:
        the result of the SQL query
    """
    _log("Running sql: {}".format(sql_str))
    return spark.sql(sql_str)


def _get_table_name(featuregroup, version):
    """
    Gets the Hive table name of a featuregroup and version

    Args:
        :featuregroup: the featuregroup to get the table name of
        :version: the version of the featuregroup

    Returns:
        The Hive table name of the featuregroup with the specified version
    """
    return featuregroup + "_" + str(version)


def _get_feature_store_metadata(featurestore=None):
    """
    Makes a REST call to the appservice in hopsworks to get all featuregroups and training datasets for
    the provided featurestore, authenticating with keystore and password.

    Args:
        :featurestore: the name of the database, defaults to the project's featurestore

    Returns:
        JSON list of featuregroups

    """
    if featurestore is None:
        featurestore = project_featurestore()
    json_contents = tls._prepare_rest_appservice_json_request()
    json_contents[constants.REST_CONFIG.JSON_FEATURESTORENAME] = featurestore
    json_embeddable = json.dumps(json_contents)
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method = constants.HTTP_CONFIG.HTTP_POST
    connection = util._get_http_connection(https=True)
    resource = constants.REST_CONFIG.HOPSWORKS_FEATURESTORE_RESOURCE
    resource_url = constants.DELIMITERS.SLASH_DELIMITER + constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + constants.REST_CONFIG.HOPSWORKS_REST_APPSERVICE + constants.DELIMITERS.SLASH_DELIMITER + resource
    connection.request(method, resource_url, json_embeddable, headers)
    response = connection.getresponse()
    resp_body = response.read()
    response_object = json.loads(resp_body)
    return response_object


def _parse_featuregroups_json(featuregroups):
    """
    Parses the list of JSON featuregroups into a dict {featuregroup --> {features, version}}

    Args:
        :featuregroups: a list of JSON featuregroups

    Returns:
        A list of of [{featuregroupName, features, version}]
    """
    parsed_featuregroups = []
    for fg in featuregroups:
        parsed_fg = {}
        parsed_fg[constants.REST_CONFIG.JSON_FEATUREGROUPNAME] = fg[constants.REST_CONFIG.JSON_FEATUREGROUPNAME]
        parsed_fg[constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES] = fg[
            constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES]
        parsed_fg[constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION] = fg[constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION]
        parsed_featuregroups.append(parsed_fg)
    return parsed_featuregroups


def _find_featuregroup_that_contains_feature(featuregroups, feature):
    """
    Go through list of featuregroups and find the ones that contain the feature

    Args:
        :featuregroups: featuregroups to search through
        :feature: the feature to look for

    Returns:
        a list of featuregroup names and versions for featuregroups that contain the given feature

    """
    matches = []
    for fg in featuregroups:
        for f in fg[constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES]:
            fg_table_name = _get_table_name(fg[constants.REST_CONFIG.JSON_FEATUREGROUPNAME],
                                            fg[constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION])
            full_name = fg_table_name + "." + f[constants.REST_CONFIG.JSON_FEATURE_NAME]
            if f[constants.REST_CONFIG.JSON_FEATURE_NAME] == feature or full_name == feature:
                matches.append(fg)
                break
    return matches


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
        featurestore = project_featurestore()
    try:
        sql_str = "use " + featurestore
        _run_and_log_sql(spark, sql_str)
    except AnalysisException as e:
        raise AssertionError((
            "A hive database for the featurestore {} was not found, have you enabled the featurestore service in your project?".format(
                featurestore)))


def _return_dataframe_type(dataframe, dataframe_type):
    """
    Helper method for returning te dataframe in spark/pandas/numpy/python, depending on user preferences

    Args:
        :dataframe: the spark dataframe to convert
        :dataframe_type: the type to convert to (spark,pandas,numpy,python)

    Returns:
        The dataframe converted to either spark, pandas, numpy or python.
    """
    if dataframe_type == constants.FEATURE_STORE.DATAFRAME_TYPE_SPARK:
        return dataframe
    if dataframe_type == constants.FEATURE_STORE.DATAFRAME_TYPE_PANDAS:
        return dataframe.toPandas()
    if dataframe_type == constants.FEATURE_STORE.DATAFRAME_TYPE_NUMPY:
        return np.array(dataframe.collect())
    if dataframe_type == constants.FEATURE_STORE.DATAFRAME_TYPE_PYTHON:
        return dataframe.collect()


def _convert_dataframe_to_spark(dataframe):
    """
    Helper method for converting a user-provided dataframe into a spark dataframe

    Args:
        :dataframe: the input dataframe (supported types are spark rdds, spark dataframes, pandas dataframes, python 2D lists, and numpy 2D arrays)

    Returns:
        the dataframe convertd to a spark dataframe
    """
    spark = util._find_spark()
    if isinstance(dataframe, pd.DataFrame):
        sc = spark.sparkContext
        sql_context = SQLContext(sc)
        return sql_context.createDataFrame(dataframe)
    if isinstance(dataframe, list):
        dataframe = np.array(dataframe)
    if isinstance(dataframe, np.ndarray):
        if dataframe.ndim != 2:
            raise AssertionError(
                "Cannot convert numpy array that do not have two dimensions to a dataframe. The number of dimensions are: {}".format(
                    dataframe.ndim))
        num_cols = dataframe.shape[1]
        dataframe_dict = {}
        for n_col in list(range(num_cols)):
            col_name = "col_" + str(n_col)
            dataframe_dict[col_name] = dataframe[:, n_col]
        pandas_df = pd.DataFrame(dataframe_dict)
        sc = spark.sparkContext
        sql_context = SQLContext(sc)
        return sql_context.createDataFrame(pandas_df)
    if isinstance(dataframe, RDD):
        return dataframe.toDF()
    if isinstance(dataframe, DataFrame):
        return dataframe
    raise AssertionError(
        "The provided dataframe type is not recognized. Supported types are: spark rdds, spark dataframes, pandas dataframes, python 2D lists, and numpy 2D arrays. The provided dataframe has type: {}".format(
            type(dataframe)))


def get_featuregroup(featuregroup, featurestore=None, featuregroup_version=1, dataframe_type="spark"):
    """
    Gets a featuregroup from a featurestore as a spark dataframe

    Example usage:

    >>> #The API will default to version 1 for the feature group and the project's own feature store
    >>> trx_summary_features = featurestore.get_featuregroup("trx_summary_features")
    >>> #You can also explicitly define version and feature store:
    >>> trx_summary_features = featurestore.get_featuregroup("trx_summary_features", featurestore=featurestore.project_featurestore(), featuregroup_version = 1)

    Args:
        :featuregroup: the featuregroup to get
        :featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        :featuregroup_version: (Optional) the version of the featuregroup
        :dataframe_type: the type of the returned dataframe (spark, pandas, python or numpy)

    Returns:
        a spark dataframe with the contents of the featurestore

    """
    if featurestore is None:
        featurestore = project_featurestore()
    spark = util._find_spark()
    spark.sparkContext.setJobGroup("Fetching Featuregroup",
                                   "Getting feature group: {} from the featurestore {}".format(featuregroup,
                                                                                               featurestore))
    _use_featurestore(spark, featurestore)
    sql_str = "SELECT * FROM " + _get_table_name(featuregroup, featuregroup_version)
    result = _run_and_log_sql(spark, sql_str)
    spark.sparkContext.setJobGroup("", "")
    return _return_dataframe_type(result, dataframe_type)


def _find_feature(feature, featurestore, featuregroups_parsed):
    """
    Looks if a given feature can be uniquely found in a list of featuregroups and returns that featuregroup.
    Otherwise it throws an exception

    Args:
        :feature: the feature to search for
        :featurestore: the featurestore where the featuregroups resides
        :featuregroups_parsed: the featuregroups to look through

    Returns:
        the featuregroup that contains the feature

    """
    featuregroups_matched = _find_featuregroup_that_contains_feature(featuregroups_parsed, feature)
    if (len(featuregroups_matched) == 0):
        raise AssertionError(
            "Could not find the feature with name '{}' in any of the featuregroups of the featurestore: '{}'".format(
                feature, featurestore))
    if (len(featuregroups_matched) > 1):
        featuregroups_matched_str_list = map(lambda fg: _get_table_name(fg[constants.REST_CONFIG.JSON_FEATUREGROUPNAME],
                                                                        fg[
                                                                            constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION]),
                                             featuregroups_matched)
        featuregroups_matched_str = ",".join(featuregroups_matched_str_list)
        raise AssertionError("Found the feature with name '{}' " \
                             "in more than one of the featuregroups of the featurestore: '{}', " \
                             "please specify the optional argument 'featuregroup=', " \
                             "the matched featuregroups were: {}".format(feature, featurestore,
                                                                         featuregroups_matched_str))
    return featuregroups_matched[0]


def get_feature(feature, featurestore=None, featuregroup=None, featuregroup_version=1, dataframe_type="spark"):
    """
    Gets a particular feature (column) from a featurestore, if no featuregroup is specified it queries hopsworks metastore
    to see if the feature exists in any of the featuregroups in the featurestore. If the user knows which featuregroup
    contain the feature, it should be specified as it will improve performance of the query.

    Example usage:

    >>> #The API will infer the featuregroup and default to version 1 for the feature group with this and the project's own feature store
    >>> max_trx_feature = featurestore.get_feature("max_trx")
    >>> #You can also explicitly define feature group,version and feature store:
    >>> max_trx_feature = featurestore.get_feature("max_trx", featurestore=featurestore.project_featurestore(), featuregroup="trx_summary_features", featuregroup_version = 1)

    Args:
        :feature: the feature name to get
        :featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        :featuregroup: (Optional) the featuregroup where the feature resides
        :featuregroup_version: (Optional) the version of the featuregroup
        :dataframe_type: the type of the returned dataframe (spark, pandas, python or numpy)

    Returns:
        A spark dataframe with the feature

    """
    if featurestore is None:
        featurestore = project_featurestore()
    spark = util._find_spark()
    _use_featurestore(spark, featurestore)
    spark.sparkContext.setJobGroup("Fetching Feature",
                                   "Getting feature: {} from the featurestore {}".format(feature, featurestore))
    if (featuregroup != None):
        sql_str = "SELECT " + feature + " FROM " + _get_table_name(featuregroup, featuregroup_version)
        result = _run_and_log_sql(spark, sql_str)
        return _return_dataframe_type(result, dataframe_type)
    else:
        # make REST call to find out where the feature is located and return them
        # if the feature exists in multiple tables return an error message specifying this
        featuregroups_json = _get_feature_store_metadata(featurestore)["featuregroups"]
        featuregroups_parsed = _parse_featuregroups_json(featuregroups_json)
        if (len(featuregroups_parsed) == 0):
            raise AssertionError("Could not find any featuregroups in the metastore, " \
                                 "please explicitly supply featuregroups as an argument to the API call")
        featuregroup_matched = _find_feature(feature, featurestore, featuregroups_parsed)
        sql_str = "SELECT " + feature + " FROM " + _get_table_name(
            featuregroup_matched[constants.REST_CONFIG.JSON_FEATUREGROUPNAME],
            featuregroup_matched[constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION])
        result = _run_and_log_sql(spark, sql_str)
        spark.sparkContext.setJobGroup("Fetching Feature",
                                       "Getting feature: {} from the featurestore {}".format(feature, featurestore))
        return _return_dataframe_type(result, dataframe_type)


def _get_join_str(featuregroups, join_key):
    """
    Constructs the JOIN COl,... ON X string from a list of tables (featuregroups) and join column
    Args:
        :featuregroups: the featuregroups to join
        :join_key: the key to join on

    Returns:
        SQL join string to join a set of feature groups together
    """
    join_str = ""
    for idx, fg in enumerate(featuregroups):
        if (idx != 0):
            join_str = join_str + "JOIN " + _get_table_name(fg[constants.REST_CONFIG.JSON_FEATUREGROUPNAME],
                                                            fg[constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION]) + " "
    join_str = join_str + "ON "
    for idx, fg in enumerate(featuregroups):
        if (idx != 0 and idx < (len(featuregroups) - 1)):
            join_str = join_str + _get_table_name(featuregroups[0][constants.REST_CONFIG.JSON_FEATUREGROUPNAME],
                                                  featuregroups[0][
                                                      constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION]) + ".`" + join_key + "`=" + \
                       _get_table_name(fg[constants.REST_CONFIG.JSON_FEATUREGROUPNAME],
                                       fg[constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION]) + ".`" + join_key + "` AND "
        elif (idx != 0 and idx == (len(featuregroups) - 1)):
            join_str = join_str + _get_table_name(featuregroups[0][constants.REST_CONFIG.JSON_FEATUREGROUPNAME],
                                                  featuregroups[0][
                                                      constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION]) + ".`" + join_key + "`=" + \
                       _get_table_name(fg[constants.REST_CONFIG.JSON_FEATUREGROUPNAME],
                                       fg[constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION]) + ".`" + join_key + "`"
    return join_str


def _get_col_that_is_primary(common_cols, featuregroups):
    """
    Helper method that returns the column among a shared column between featuregroups that is most often marked as
    'primary' in the hive schema.

    Args:
        :common_cols: the list of columns shared between all featuregroups
        :featuregroups: the list of featuregroups

    Returns:
        the column among a shared column between featuregroups that is most often marked as 'primary' in the hive schema
    """
    primary_counts = []
    for col in common_cols:
        primary_count = 0
        for fg in featuregroups:
            for feature in fg[constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES]:
                if feature[constants.REST_CONFIG.JSON_FEATURE_NAME] == col and feature[
                    constants.REST_CONFIG.JSON_FEATURE_PRIMARY]:
                    primary_count = primary_count + 1
        primary_counts.append(primary_count)

    max_no_primary = max(primary_counts)

    if max(primary_counts) == 0:
        return common_cols[0]
    else:
        return common_cols[primary_counts.index(max_no_primary)]


def _validate_metadata(name, dtypes, dependencies, description):
    """
    Function for validating metadata when creating new feature groups and training datasets.
    Raises and assertion exception if there is some error in the metadata.

    Args:
        :name: the name of the feature group/training dataset
        :dtypes: the dtypes in the provided spark dataframe
        :dependencies: the list of data dependencies
        :description: the description

    Returns:
        None
    """
    name_pattern = re.compile("^[a-zA-Z0-9-_]+$")
    if len(name) > 256 or name == "" or not name_pattern.match(name) or "-" in name:
        raise AssertionError("Name of feature group/training dataset cannot be empty, cannot exceed 256 characters," \
                             ", cannot contain hyphens ('-') and must match the regular expression: ^[a-zA-Z0-9-_]+$, the provided name: {} is not valid".format(
            name))
    if len(dtypes) == 0:
        raise AssertionError("Cannot create a feature group from an empty spark dataframe")

    for dtype in dtypes:
        if len(dtype[0]) > 767 or dtype[0] == "" or not name_pattern.match(dtype[0]) or "-" in dtype[0]:
            raise AssertionError("Name of feature column cannot be empty, cannot exceed 767 characters," \
                                 "cannot contain hyphens ('-'), and must match the regular expression: ^[a-zA-Z0-9-_]+$, the provided feature name: {} is not valid".format(
                dtype[0]))

    if not len(set(dependencies)) == len(dependencies):
        dependencies_str = ",".join(dependencies)
        raise AssertionError("The list of data dependencies contains duplicates: {}".format(dependencies_str))

    if len(description) > 2000:
        raise AssertionError(
            "Feature group/Training dataset description should not exceed the maximum length of 2000 characters, the provided description has length: {}".format(
                len(description)))


def _get_join_col(featuregroups):
    """
    Finds a common JOIN column among featuregroups (hive tables)

    Args:
        :featuregroups: a list of featuregroups with version and features

    Returns:
        name of the join column

    """
    feature_sets = []
    for fg in featuregroups:
        columns = fg[constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES]
        columns_names = map(lambda col: col[constants.REST_CONFIG.JSON_FEATURE_NAME], columns)
        feature_set = set(columns_names)
        feature_sets.append(feature_set)

    common_cols = list(set.intersection(*feature_sets))

    if (len(common_cols) == 0):
        featuregroups_str = ", ".join(
            list(map(lambda x: x[constants.REST_CONFIG.JSON_FEATUREGROUPNAME], featuregroups)))
        raise AssertionError("Could not find any common columns in featuregroups to join on, " \
                             "searched through featuregroups: " \
                             "{}".format(featuregroups_str))
    return _get_col_that_is_primary(common_cols, featuregroups)


def _convert_featuregroup_version_dict(featuregroups_version_dict):
    """
    Converts a featuregroup->version dict into a list of {name: name, version: version}

    Args:
        :featuregroups_version_dict:

    Returns:
        a list of {featuregroup_name: name, version: version}

    """
    parsed_featuregroups = []
    for i, (name, version) in enumerate(featuregroups_version_dict.items()):
        parsed_featuregroup = {}
        parsed_featuregroup[constants.REST_CONFIG.JSON_FEATUREGROUPNAME] = name
        parsed_featuregroup[constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION] = version
        parsed_featuregroups.append(parsed_featuregroup)
    return parsed_featuregroups


def get_features(features, featurestore=None, featuregroups_version_dict={}, join_key=None, dataframe_type="spark"):
    """
    Gets a list of features (columns) from the featurestore. If no featuregroup is specified it will query hopsworks
    metastore to find where the features are stored.

    Example usage:

    >>> # The API will default to version 1 for feature groups and the project's feature store
    >>> features = featurestore.get_features(["pagerank", "triangle_count", "avg_trx"], featurestore=featurestore.project_featurestore())
    >>> #You can also explicitly define feature group, version, feature store, and join-key:
    >>> features = featurestore.get_features(["pagerank", "triangle_count", "avg_trx"], featurestore=featurestore.project_featurestore(), featuregroups_version_dict={"trx_graph_summary_features": 1, "trx_summary_features": 1}, join_key="cust_id")

    Args:
        :features: a list of features to get from the featurestore
        :featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        :featuregroups: (Optional) a dict with (fg --> version) for all the featuregroups where the features resides
        :featuregroup_version: (Optional) the version of the featuregroup
        :join_key: (Optional) column name to join on
        :dataframe_type: the type of the returned dataframe (spark, pandas, python or numpy)

    Returns:
        A spark dataframe with all the features

    """
    if featurestore is None:
        featurestore = project_featurestore()
    features = list(set(features))
    spark = util._find_spark()
    _use_featurestore(spark, featurestore)
    spark.sparkContext.setJobGroup("Fetching Features",
                                   "Getting features: {} from the featurestore {}".format(features, featurestore))
    featuresStr = ", ".join(features)
    featuregroupsStrings = []
    for fg in featuregroups_version_dict:
        featuregroupsStrings.append(_get_table_name(fg, featuregroups_version_dict[fg]))
    featuregroupssStr = ", ".join(featuregroupsStrings)

    if (len(featuregroups_version_dict) == 1):
        sql_str = "SELECT " + featuresStr + " FROM " + featuregroupssStr
        result = _run_and_log_sql(spark, sql_str)
        return _return_dataframe_type(result, dataframe_type)

    if (len(featuregroups_version_dict) > 1):
        if (join_key != None):
            featuregroups_parsed_filtered = _convert_featuregroup_version_dict(featuregroups_version_dict)
            join_str = _get_join_str(featuregroups_parsed_filtered, join_key)
        else:
            featuregroups_json = _get_feature_store_metadata(featurestore)[constants.REST_CONFIG.JSON_FEATUREGROUPS]
            featuregroups_parsed = _parse_featuregroups_json(featuregroups_json)
            if (len(featuregroups_parsed) == 0):
                raise AssertionError("Could not find any featuregroups in the metastore, " \
                                     "please explicitly supply featuregroups as an argument to the API call")
            featuregroups_parsed_filtered = list(filter(
                lambda fg: fg[constants.REST_CONFIG.JSON_FEATUREGROUPNAME] in featuregroups_version_dict and
                           featuregroups_version_dict[
                               fg[constants.REST_CONFIG.JSON_FEATUREGROUPNAME]] == fg[
                               constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION],
                featuregroups_parsed))
            join_col = _get_join_col(featuregroups_parsed_filtered)
            join_str = _get_join_str(featuregroups_parsed_filtered, join_col)

        sql_str = "SELECT " + featuresStr + " FROM " + _get_table_name(
            featuregroups_parsed_filtered[0][constants.REST_CONFIG.JSON_FEATUREGROUPNAME],
            featuregroups_parsed_filtered[0][constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION]) \
                  + " " + join_str
        result = _run_and_log_sql(spark, sql_str)
        spark.sparkContext.setJobGroup("", "")
        return _return_dataframe_type(result, dataframe_type)

    if (len(featuregroups_version_dict) == 0):
        # make REST call to find out where the feature is located and return them
        # if the feature exists in multiple tables return an error message specifying this
        featuregroups_json = _get_feature_store_metadata(featurestore)[constants.REST_CONFIG.JSON_FEATUREGROUPS]
        featuregroups_parsed = _parse_featuregroups_json(featuregroups_json)
        if (len(featuregroups_parsed) == 0):
            raise AssertionError("Could not find any featuregroups in the metastore, " \
                                 "please explicitly supply featuregroups as an argument to the API call")
        feature_to_featuregroup = {}
        feature_featuregroups = []
        for feature in features:
            featuregroup_matched = _find_feature(feature, featurestore, featuregroups_parsed)
            feature_to_featuregroup[feature] = featuregroup_matched
            if not _check_if_list_of_featuregroups_contains_featuregroup(feature_featuregroups,
                                                                         featuregroup_matched[
                                                                             constants.REST_CONFIG.JSON_FEATUREGROUPNAME],
                                                                         featuregroup_matched[
                                                                             constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION]):
                feature_featuregroups.append(featuregroup_matched)
        if len(feature_featuregroups) == 1:
            sql_str = "SELECT " + featuresStr + " FROM " + _get_table_name(
                feature_featuregroups[0][constants.REST_CONFIG.JSON_FEATUREGROUPNAME],
                feature_featuregroups[0][constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION])
        else:
            join_col = _get_join_col(feature_featuregroups)
            join_str = _get_join_str(feature_featuregroups, join_col)
            sql_str = "SELECT " + featuresStr + " FROM " + _get_table_name(
                feature_featuregroups[0][constants.REST_CONFIG.JSON_FEATUREGROUPNAME],
                feature_featuregroups[0][constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION]) + " " \
                      + join_str
        result = _run_and_log_sql(spark, sql_str)
        return _return_dataframe_type(result, dataframe_type)


def _check_if_list_of_featuregroups_contains_featuregroup(featuregroups, featuregroupname, version):
    """
    Check if a list of featuregroup contains a featuregroup with a particular name and version

    Args:
        :featuregroups: the list of featuregroups to search through
        :featuregroupname: the name of the featuregroup
        :version: the featuregroup version

    Returns:
        boolean indicating whether the featuregroup name and version exists in the list
    """
    match_bool = False
    for fg in featuregroups:
        if (fg[constants.REST_CONFIG.JSON_FEATUREGROUPNAME] == featuregroupname and fg[
            constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION] == version):
            match_bool = True
    return match_bool


def sql(query, featurestore=None, dataframe_type="spark"):
    """
    Executes a generic SQL query on the featurestore

    Example usage:

    >>> # The API will default to the project's feature store
    >>> featurestore.sql("SELECT * FROM trx_graph_summary_features_1 WHERE triangle_count > 5").show(5)
    >>> # You can also explicitly define the feature store
    >>> featurestore.sql("SELECT * FROM trx_graph_summary_features_1 WHERE triangle_count > 5", featurestore=featurestore.project_featurestore()).show(5)

    Args:
        :query: SQL query
        :featurestore: the featurestore to query, defaults to the project's featurestore
        :dataframe_type: the type of the returned dataframe (spark, pandas, python or numpy)

    Returns:
        A dataframe with the query results

    """
    if featurestore is None:
        featurestore = project_featurestore()
    spark = util._find_spark()
    spark.sparkContext.setJobGroup("Running SQL query against feature store",
                                   "Running query: {} on the featurestore {}".format(query, featurestore))
    _use_featurestore(spark, featurestore)
    result = _run_and_log_sql(spark, query)
    spark.sparkContext.setJobGroup("", "")
    return _return_dataframe_type(result, dataframe_type)


def _delete_table_contents(featurestore, featuregroup, featuregroup_version):
    """
    Sends a request to clear the contents of a featuregroup by dropping the featuregroup and recreating it with
    the same metadata.

    Args:
        :featurestore: the featurestore where the featuregroup resides
        :featuregroup: the featuregroup to clear
        :featuregroup_version: the version of the featuregroup

    Returns:
        The JSON response

    """
    json_contents = tls._prepare_rest_appservice_json_request()
    json_contents[constants.REST_CONFIG.JSON_FEATURESTORENAME] = featurestore
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUPNAME] = featuregroup
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION] = featuregroup_version
    json_embeddable = json.dumps(json_contents)
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method = constants.HTTP_CONFIG.HTTP_POST
    connection = util._get_http_connection(https=True)
    resource = constants.REST_CONFIG.HOPSWORKS_CLEAR_FEATUREGROUP_RESOURCE
    resource_url = constants.DELIMITERS.SLASH_DELIMITER + constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + constants.REST_CONFIG.HOPSWORKS_REST_APPSERVICE + constants.DELIMITERS.SLASH_DELIMITER + resource
    connection.request(method, resource_url, json_embeddable, headers)
    response = connection.getresponse()
    resp_body = response.read()
    response_object = json.loads(resp_body)
    try:  # for python 3
        if (response.code != 200):
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise AssertionError("Could not clear featuregroup contents, server response: \n " \
                                 "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                response.code, response.reason, error_code, error_msg, user_msg))
    except:  # for python 2
        if (response.status != 200):
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise AssertionError("Could not clear featuregroup contents, server response: \n " \
                                 "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                response.code, response.reason, error_code, error_msg, user_msg))
    return response_object


def _get_featurestores():
    """
    Sends a REST request to get all featurestores for the project

    Returns:
        a list of Featurestore JSON DTOs
    """
    json_contents = tls._prepare_rest_appservice_json_request()
    json_embeddable = json.dumps(json_contents)
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method = constants.HTTP_CONFIG.HTTP_POST
    connection = util._get_http_connection(https=True)
    resource = constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE
    resource_url = constants.DELIMITERS.SLASH_DELIMITER + constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + constants.REST_CONFIG.HOPSWORKS_REST_APPSERVICE + constants.DELIMITERS.SLASH_DELIMITER + resource
    connection.request(method, resource_url, json_embeddable, headers)
    response = connection.getresponse()
    resp_body = response.read()
    response_object = json.loads(resp_body)
    try:  # for python 3
        if (response.code != 200):
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise AssertionError("Could not fetch feature stores, server response: \n " \
                                 "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                response.code, response.reason, error_code, error_msg, user_msg))
    except:  # for python 2
        if (response.status != 200):
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise AssertionError("Could not fetch feature stores, server response: \n " \
                                 "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                response.code, response.reason, error_code, error_msg, user_msg))
    return response_object

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
    spark.sparkContext.setJobGroup("Inserting dataframe into featuregroup",
                                   "Inserting into featuregroup: {} in the featurestore {}".format(featuregroup,
                                                                                                   featurestore))
    _use_featurestore(spark, featurestore)
    tbl_name = _get_table_name(featuregroup, featuregroup_version)
    if (mode == constants.FEATURE_STORE.FEATURE_GROUP_INSERT_OVERWRITE_MODE):
        _delete_table_contents(featurestore, featuregroup, featuregroup_version)

    if (
                not mode == constants.FEATURE_STORE.FEATURE_GROUP_INSERT_APPEND_MODE and not mode == constants.FEATURE_STORE.FEATURE_GROUP_INSERT_OVERWRITE_MODE):
        raise AssertionError(
            "The provided write mode {} does not match the supported modes: ['{}', '{}']".format(mode,
                                                                                                 constants.FEATURE_STORE.FEATURE_GROUP_INSERT_APPEND_MODE,
                                                                                                 constants.FEATURE_STORE.FEATURE_GROUP_INSERT_OVERWRITE_MODE))
    # overwrite is not supported because it will drop the table and create a new one,
    # this means that all the featuregroup metadata will be dropped due to ON DELETE CASCADE
    # to simulate "overwrite" we call appservice to drop featuregroup and re-create with the same metadata
    mode = constants.FEATURE_STORE.FEATURE_GROUP_INSERT_APPEND_MODE
    # Specify format hive as it is managed table
    format = "hive"
    spark_df.write.format(format).mode(mode).saveAsTable(tbl_name)
    spark.sparkContext.setJobGroup("", "")

def insert_into_featuregroup(df, featuregroup, featurestore=None, featuregroup_version=1, mode="append",
                             descriptive_statistics=True, feature_correlation=True, feature_histograms=True,
                             cluster_analysis=True, stat_columns=None, num_bins=20, corr_method='pearson',
                             num_clusters=5):
    """
    Saves the given dataframe to the specified featuregroup. Defaults to the project-featurestore
    This will append to  the featuregroup. To overwrite a featuregroup, create a new version of the featuregroup
    from the UI and append to that table.

    Example usage:

    >>> # The API will default to the project's feature store, featuegroup version 1, and write mode 'append'
    >>> featurestore.insert_into_featuregroup(sampleDf, "trx_graph_summary_features")
    >>> # You can also explicitly define the feature store, the featuregroup version, and the write mode (only append and overwrite are supported)
    >>> featurestore.insert_into_featuregroup(sampleDf, "trx_graph_summary_features", featurestore=featurestore.project_featurestore(), featuregroup_version=1, mode="append",     >>> featurestore.insert_into_featuregroup(sampleDf, "trx_graph_summary_features", featurestore=featurestore.project_featurestore(), featuregroup_version=1, mode="append", descriptive_statistics=True, feature_correlation=True, feature_histograms=True, cluster_analysis=True, stat_columns=None))

    Args:
        :df: the dataframe containing the data to insert into the featuregroup
        :featuregroup: the name of the featuregroup (hive table name)
        :featurestore: the featurestore to save the featuregroup to (hive database)
        :featuregroup_version: the version of the featuregroup (defaults to 1)
        :mode: the write mode, only 'overwrite' and 'append' are supported
        :descriptive_statistics: a boolean flag whether to compute descriptive statistics (min,max,mean etc) for the featuregroup
        :feature_correlation: a boolean flag whether to compute a feature correlation matrix for the numeric columns in the featuregroup
        :feature_histograms: a boolean flag whether to compute histograms for the numeric columns in the featuregroup
        :cluster_analysis: a boolean flag whether to compute cluster analysis for the numeric columns in the featuregroup
        :stat_columns: a list of columns to compute statistics for (defaults to all columns that are numeric)
        :num_bins: number of bins to use for computing histograms
        :num_clusters: number of clusters to use for cluster analysis
        :corr_method: the method to compute feature correlation with (pearson or spearman)

    Returns:
        None
    """
    try:
        spark_df = _convert_dataframe_to_spark(df)
    except Exception as e:
        raise AssertionError("Could not convert the provided dataframe to a spark dataframe which is required in order to save it to the Feature Store, error: {}".format(str(e)))

    if featurestore is None:
        featurestore = project_featurestore()

    feature_corr_data, featuregroup_desc_stats_data, features_histogram_data, cluster_analysis_data = _compute_dataframe_stats(
        featuregroup, spark_df=spark_df, version=featuregroup_version, featurestore=featurestore,
        descriptive_statistics=descriptive_statistics, feature_correlation=feature_correlation,
        feature_histograms=feature_histograms, cluster_analysis=cluster_analysis, stat_columns=stat_columns,
        num_bins=num_bins, corr_method=corr_method,
        num_clusters=num_clusters)
    _write_featuregroup_hive(spark_df, featuregroup, featurestore, featuregroup_version, mode)
    _update_featuregroup_stats_rest(featuregroup, featurestore, featuregroup_version, feature_corr_data,
                                    featuregroup_desc_stats_data, features_histogram_data, cluster_analysis_data)


def _convert_spark_dtype_to_hive_dtype(spark_dtype):
    """
    Helper function to convert a spark data type into a hive datatype

    Args:
        :spark_dtype: the spark datatype to convert

    Returns:
        the hive datatype or None

    """
    if type(spark_dtype) is dict:
        if spark_dtype[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELD_TYPE].lower() == constants.SPARK_CONFIG.SPARK_ARRAY:
            return spark_dtype[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELD_TYPE] + "<" + _convert_spark_dtype_to_hive_dtype(spark_dtype[constants.SPARK_CONFIG.SPARK_SCHEMA_ELEMENT_TYPE]) + ">"
        if spark_dtype[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELD_TYPE].lower() == constants.SPARK_CONFIG.SPARK_STRUCT:
            struct_nested_fields = list(map(
                lambda field: field[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELD_NAME] +
                              constants.DELIMITERS.COLON_DELIMITER +  _convert_spark_dtype_to_hive_dtype(field[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELD_TYPE]),
                spark_dtype[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELDS]))
            return spark_dtype[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELD_TYPE] + "<" + constants.DELIMITERS.COMMA_DELIMITER.join(struct_nested_fields) + ">"
    if spark_dtype.upper() in constants.HIVE_CONFIG.HIVE_DATA_TYPES:
        return spark_dtype.upper()
    if spark_dtype.lower() == constants.SPARK_CONFIG.SPARK_LONG_TYPE:
        return constants.HIVE_CONFIG.HIVE_BIGINT_TYPE
    if spark_dtype.lower() == constants.SPARK_CONFIG.SPARK_SHORT_TYPE:
        return constants.HIVE_CONFIG.HIVE_INT_TYPE
    if spark_dtype.lower() == constants.SPARK_CONFIG.SPARK_BYTE_TYPE:
        return constants.HIVE_CONFIG.HIVE_CHAR_TYPE
    if spark_dtype.lower() == constants.SPARK_CONFIG.SPARK_INTEGER_TYPE:
        return constants.HIVE_CONFIG.HIVE_INT_TYPE
    if constants.SPARK_CONFIG.SPARK_DECIMAL_TYPE in spark_dtype.lower():
        return spark_dtype
    raise AssertionError("Dataframe data type: {} not recognized.".format(spark_dtype))


def _convert_field_to_feature(field_dict, primary_key):
    """
    Helper function that converts a field in a spark dataframe to a feature dict that is compatible with the
     featurestore API

    Args:
        :field_dict: the dict of spark field to convert
        :primary_key: name of the primary key feature

    Returns:
        a feature dict that is compatible with the featurestore API

    """
    f_name = field_dict[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELD_NAME]
    f_type = _convert_spark_dtype_to_hive_dtype(field_dict[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELD_TYPE])
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
    return {
        constants.REST_CONFIG.JSON_FEATURE_NAME: f_name,
        constants.REST_CONFIG.JSON_FEATURE_TYPE: f_type,
        constants.REST_CONFIG.JSON_FEATURE_DESCRIPTION: f_desc,
        constants.REST_CONFIG.JSON_FEATURE_PRIMARY: f_primary
    }


def _parse_spark_features_schema(spark_schema, primary_key):
    """
    Helper function for parsing the schema of a spark dataframe into a list of feature-dicts

    Args:
        :spark_schema: the spark schema to parse
        :primary_key: the column in the dataframe that should be the primary key

    Returns:
        A list of the parsed features

    """
    raw_schema = json.loads(spark_schema.json())
    raw_fields = raw_schema[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELDS]
    parsed_features = list(map(lambda field: _convert_field_to_feature(field, primary_key), raw_fields))
    return parsed_features


def _compute_corr_matrix(spark_df, corr_method='pearson'):
    """
    A helper function for computing a correlation matrix of a spark dataframe (works only with numeric columns).
    The correlation matrix represents the pair correlation of all the variables. By default the method will use
    Pearson correlation (a measure of the linear correlation between two variables X and Y,
    it has a value between +1 and -1, where 1 is total positive linear correlation,
    0 is no linear correlation, and -1 is total negative linear correlation).

    The correlation matrix is computed with Spark.

    Args:
        :spark_df: the spark dataframe to compute the correlation matrix for
        :method: the correlation method, defaults to pearson (spearman supported as well)

    Returns:
        a pandas dataframe with the correlation matrix

    """
    numeric_columns = spark_df.dtypes
    if (len(numeric_columns) == 0):
        raise AssertionError("The provided spark dataframe does not contain any numeric columns. " \
                             "Cannot compute feature correlation on categorical columns. The numeric datatypes are: {}" \
                             " and the number of numeric datatypes in the dataframe is: {} ({})".format(
            constants.SPARK_CONFIG.SPARK_NUMERIC_TYPES, len(spark_df.dtypes), spark_df.dtypes))
    if (len(numeric_columns) == 1):
        raise AssertionError("The provided spark dataframe only contains one numeric column. " \
                             "Cannot compute feature correlation on just one column. The numeric datatypes are: {}" \
                             "and the number of numeric datatypes in the dataframe is: {} ({})".format(
            constants.SPARK_CONFIG.SPARK_NUMERIC_TYPES, len(spark_df.dtypes), spark_df.dtypes))
    if (len(numeric_columns) > constants.FEATURE_STORE.MAX_CORRELATION_MATRIX_COLUMNS):
        raise AssertionError("The provided dataframe contains  {} columns, feature correlation can only be computed for dataframes with < {} columns due to scalability reasons (number of correlatons grows quadratically with the number of columns)".format(len(numeric_columns), constants.FEATURE_STORE.MAX_CORRELATION_MATRIX_COLUMNS))
    spark_df_rdd = spark_df.rdd.map(lambda row: row[0:])
    corr_mat = Statistics.corr(spark_df_rdd, method=corr_method)
    pd_df_corr_mat = pd.DataFrame(corr_mat, columns=spark_df.columns, index=spark_df.columns)
    return pd_df_corr_mat


def _compute_cluster_analysis(spark_df, clusters=5):
    numeric_columns = list(map(lambda col_dtype: col_dtype[0], spark_df.dtypes))
    if (len(numeric_columns) == 0):
        raise AssertionError("The provided spark dataframe does not contain any numeric columns. " \
                             "Cannot compute cluster analysis with k-means on categorical columns. "
                             "The numeric datatypes are: {}" \
                             " and the number of numeric datatypes in the dataframe is: {} ({})".format(
            constants.SPARK_CONFIG.SPARK_NUMERIC_TYPES, len(spark_df.dtypes), spark_df.dtypes))
    if (len(numeric_columns) == 1):
        raise AssertionError("The provided spark dataframe does contains only one numeric column. " \
                             "Cluster analysis will filter out numeric columns and then "
                             "use pca to reduce dataset dimension to 2 dimensions and "
                             "then apply KMeans, this is not possible when the input data have only one numeric column."
                             "The numeric datatypes are: {}" \
                             " and the number of numeric datatypes in the dataframe is: {} ({})".format(
            constants.SPARK_CONFIG.SPARK_NUMERIC_TYPES, len(spark_df.dtypes), spark_df.dtypes))
    vecAssembler = VectorAssembler(inputCols=numeric_columns,
                                   outputCol=constants.FEATURE_STORE.CLUSTERING_ANALYSIS_INPUT_COLUMN)
    spark_df_1 = vecAssembler.transform(spark_df)
    kmeans = KMeans(k=clusters, seed=1, maxIter=20,
                    featuresCol=constants.FEATURE_STORE.CLUSTERING_ANALYSIS_INPUT_COLUMN,
                    predictionCol=constants.FEATURE_STORE.CLUSTERING_ANALYSIS_OUTPUT_COLUMN)
    model = kmeans.fit(spark_df_1.select(constants.FEATURE_STORE.CLUSTERING_ANALYSIS_INPUT_COLUMN))
    spark_df_2 = model.transform(spark_df_1)
    spark_df_3 = spark_df_2.select([constants.FEATURE_STORE.CLUSTERING_ANALYSIS_INPUT_COLUMN,
                                    constants.FEATURE_STORE.CLUSTERING_ANALYSIS_OUTPUT_COLUMN])
    count = spark_df_3.count()
    if count < constants.FEATURE_STORE.CLUSTERING_ANALYSIS_SAMPLE_SIZE:
        spark_df_4 = spark_df_3
    else:
        spark_df_4 = spark_df_3.sample(True,
                                       float(constants.FEATURE_STORE.CLUSTERING_ANALYSIS_SAMPLE_SIZE) / float(count))

    pca = PCA(k=2,
              inputCol=constants.FEATURE_STORE.CLUSTERING_ANALYSIS_INPUT_COLUMN,
              outputCol=constants.FEATURE_STORE.CLUSTERING_ANALYSIS_PCA_COLUMN)
    model = pca.fit(spark_df_4)
    spark_df_5 = model.transform(spark_df_4).select([constants.FEATURE_STORE.CLUSTERING_ANALYSIS_PCA_COLUMN,
                                                     constants.FEATURE_STORE.CLUSTERING_ANALYSIS_OUTPUT_COLUMN])
    spark_df_6 = spark_df_5.withColumnRenamed(
        constants.FEATURE_STORE.CLUSTERING_ANALYSIS_PCA_COLUMN,
        constants.FEATURE_STORE.CLUSTERING_ANALYSIS_FEATURES_COLUMN)
    spark_df_7 = spark_df_6.withColumnRenamed(constants.FEATURE_STORE.CLUSTERING_ANALYSIS_OUTPUT_COLUMN, "clusters")
    return json.loads(spark_df_7.toPandas().to_json())


def _compute_descriptive_statistics(spark_df):
    """
    A helper function that computes descriptive statistics for a featuregroup/training dataset using Spark

    Args:
        :spark_df: the featuregroup to compute descriptive statistics for

    Returns:
        A JSON representation of the descriptive statistics

    """
    desc_stats_json = spark_df.describe().toJSON().collect()
    return desc_stats_json


def _filter_spark_df_numeric(spark_df):
    """
    Helper function that selects only the numeric columns of a spark dataframe

    Args:
        :spark_df: the spark dataframe to filter

    Returns:
        a spark dataframe with all the numeric columns in the input-dataframe

    """
    numeric_columns = list(
        map(lambda y: y[0], filter(lambda x: x[1] in constants.SPARK_CONFIG.SPARK_NUMERIC_TYPES, spark_df.dtypes)))
    filtered_spark_df = spark_df.select(numeric_columns)
    return filtered_spark_df

def _is_type_numeric(type):
    """
    Checks whether a given type in a spark dataframe is numeric. Matches on part of string to deal with variable types
    like decimal(x,y)

    Args:
        :type: the type to check

    Returns:
        True if the type is numeric otherwise False

    """
    for spark_numeric_type in constants.SPARK_CONFIG.SPARK_NUMERIC_TYPES:
        if constants.SPARK_CONFIG.SPARK_ARRAY.lower() in type[1].lower() or \
                        constants.SPARK_CONFIG.SPARK_STRUCT.lower() in type[1].lower():
            return False
        if spark_numeric_type.lower() in type[1].lower():
            return True
    return False


def _compute_feature_histograms(spark_df, num_bins=20):
    """
    Helper function that computes histograms for all numeric features in the featuregroup/training dataset.

    The computation is done with spark and the rdd.histogram function with num_bins buckets.

    The function computes a histogram using the provided buckets.
    The buckets are all open to the right except for the last which is closed. e.g. [1,10,20,50]
    means the buckets are [1,10) [10,20) [20,50], which means 1<=x<10, 10<=x<20, 20<=x<=50.
    And on the input of 1 and 50 we would have a histogram of 1,0,1.
    If your histogram is evenly spaced (e.g. [0, 10, 20, 30]), this can be switched from an O(log n)
    inseration to O(1) per element(where n = # buckets).

    Buckets must be sorted and not contain any duplicates, must be at least two elements.
    If `buckets` is a number, it will generates buckets which are evenly spaced between
    the minimum and maximum of the RDD. For example, if the min value is 0 and the max is 100, given buckets as 2,
    the resulting buckets will be [0,50) [50,100]. buckets must be at least 1 If the RDD contains infinity,
    NaN throws an exception If the elements in RDD do not vary (max == min) always returns a single bucket.
    It will return an tuple of buckets and histogram.

    Args:
        :spark_df: the dataframe to compute the histograms for
        :num_bins: the number of bins to use in the histogram

    Returns:
        A list of histogram JSONs for all columns

    """
    histograms_json = []
    for idx, col in enumerate(spark_df.dtypes):
        col_hist = spark_df.select(col[0]).rdd.flatMap(lambda x: x).histogram(num_bins)
        col_pd_hist = pd.DataFrame(list(zip(*col_hist)), columns=['bin', 'frequency']).set_index('bin')
        col_pd_hist_json = col_pd_hist.to_json()
        col_pd_hist_dict = json.loads(col_pd_hist_json)
        col_pd_hist_dict["feature"] = col[0]
        histograms_json.append(col_pd_hist_dict)
    return histograms_json


def _compute_dataframe_stats(name, spark_df=None, version=1, featurestore=None, descriptive_statistics=True,
                             feature_correlation=True, feature_histograms=True, cluster_analysis=True,
                             stat_columns=None, num_bins=20, num_clusters=5,
                             corr_method='pearson'):
    """
    Helper function that computes statistics of a featuregroup or training dataset using spark

    Args:
        :name: the featuregroup or training dataset to update statistics for
        :spark_df: If a spark df is provided it will be used to compute statistics, otherwise the dataframe of the featuregroup will be fetched dynamically from the featurestore
        :version: the version of the featuregroup/training dataset (defaults to 1)
        :featurestore: the featurestore where the featuregroup or training dataset resides
        :descriptive_statistics: a boolean flag whether to compute descriptive statistics (min,max,mean etc) for the featuregroup/training dataset
        :feature_correlation: a boolean flag whether to compute a feature correlation matrix for the numeric columns in the featuregroup/training dataset
        :feature_histograms: a boolean flag whether to compute histograms for the numeric columns in the featuregroup/training dataset
        :cluster_analysis: a boolean flag whether to compute cluster analysis for the numeric columns in the featuregroup/training dataset
        :stat_columns: a list of columns to compute statistics for (defaults to all columns that are numeric)
        :num_bins: number of bins to use for computing histograms
        :num_clusters: the number of clusters to use for cluster analysis (k-means)
        :corr_method: the method to compute feature correlation with (pearson or spearman)

    Returns:
        feature_corr_data, desc_stats_data, features_histograms_data, cluster_analysis

    """
    if featurestore is None:
        featurestore = project_featurestore()
    if spark_df is None:
        spark_df = get_featuregroup(name, featurestore, version)
    if not stat_columns is None:
        spark_df = spark_df.select(stat_columns)
    feature_corr_data = None
    desc_stats_data = None
    features_histograms_data = None
    cluster_analysis_data = None
    spark = util._find_spark()

    if spark_df.rdd.isEmpty():
        _log("Cannot compute statistics on an empty dataframe, the provided dataframe is empty")

    if descriptive_statistics:
        try:
            _log("computing descriptive statistics for : {}".format(name))
            spark.sparkContext.setJobGroup("Descriptive Statistics Computation",
                                           "Analyzing Dataframe Statistics for : {}".format(name))
            desc_stats_json = _compute_descriptive_statistics(spark_df)
            desc_stats_data = _structure_descriptive_stats_json(desc_stats_json)
            spark.sparkContext.setJobGroup("", "")
        except Exception as e:
            _log(
                "Could not compute descriptive statistics for: {}, set the optional argument descriptive_statistics=False to skip this step,\n error: {}".format(
                    name, str(e)))

    if feature_correlation:
        try:
            _log("computing feature correlation for: {}".format(name))
            spark.sparkContext.setJobGroup("Feature Correlation Computation",
                                           "Analyzing Feature Correlations for: {}".format(name))
            spark_df_filtered = _filter_spark_df_numeric(spark_df)
            pd_corr_matrix = _compute_corr_matrix(spark_df_filtered, corr_method=corr_method)
            feature_corr_data = _structure_feature_corr_json(pd_corr_matrix.to_dict())
            spark.sparkContext.setJobGroup("", "")
        except Exception as e:
            _log(
                "Could not compute feature correlation for: {}, set the optional argument feature_correlation=False to skip this step,\n error: {}".format(
                    name, str(e)))

    if feature_histograms:
        try:
            _log("computing feature histograms for: {}".format(name))
            spark.sparkContext.setJobGroup("Feature Histogram Computation",
                                           "Analyzing Feature Distributions for: {}".format(name))
            spark_df_filtered = _filter_spark_df_numeric(spark_df)
            features_histogram_list = _compute_feature_histograms(spark_df_filtered, num_bins)
            features_histograms_data = _structure_feature_histograms_json(features_histogram_list)
            spark.sparkContext.setJobGroup("", "")
        except Exception as e:
            _log(
                "Could not compute feature histograms for: {}, set the optional argument feature_histograms=False to skip this step,\n error: {}".format(
                    name, str(e)))

    if cluster_analysis:
        try:
            _log("computing cluster analysis for: {}".format(name))
            spark.sparkContext.setJobGroup("Feature Cluster Analysis",
                                           "Analyzing Feature Clusters for: {}".format(name))
            spark_df_filtered = _filter_spark_df_numeric(spark_df)
            cluster_analysis_raw = _compute_cluster_analysis(spark_df_filtered, num_clusters)
            cluster_analysis_data = _structure_cluster_analysis_json(cluster_analysis_raw)
            spark.sparkContext.setJobGroup("", "")
        except Exception as e:
            _log(
                "Could not compute cluster analysis for: {}, set the optional argument cluster_analysis=False to skip this step,\n error: {}".format(
                    name, str(e)))

    return feature_corr_data, desc_stats_data, features_histograms_data, cluster_analysis_data


def _structure_descriptive_stats_json(descriptive_stats_list):
    """
    Converts the dict/json returned by spark descriptive statistics into the correct format that the backend
    expects in the REST call

    Args:
        :descriptive_stats_list: raw data

    Returns:
        the formatted data

    """
    descriptive_stats_list = list(map(lambda x: json.loads(x), descriptive_stats_list))
    descriptive_stats = []
    for key in descriptive_stats_list[0]:
        if not key == constants.FEATURE_STORE.DESCRIPTIVE_STATS_SUMMARY_COL:
            metric_values = []
            for ds in descriptive_stats_list:
                if key in ds:
                    try:
                        stat_value = float(ds[key])
                        if math.isnan(stat_value):
                            stat_value = None
                    except ValueError:
                        stat_value = None
                    metric_value = {
                        constants.FEATURE_STORE.DESCRIPTIVE_STATS_METRIC_NAME_COL: ds[
                            constants.FEATURE_STORE.DESCRIPTIVE_STATS_SUMMARY_COL],
                        constants.FEATURE_STORE.DESCRIPTIVE_STATS_VALUE_COL: stat_value
                    }
                    metric_values.append(metric_value)
            descriptive_stat = {
                constants.REST_CONFIG.JSON_DESCRIPTIVE_STATS_FEATURE_NAME: key,
                constants.REST_CONFIG.JSON_DESCRIPTIVE_STATS_METRIC_VALUES: metric_values
            }
            descriptive_stats.append(descriptive_stat)

    desc_stats_json_dict = {
        constants.REST_CONFIG.JSON_DESCRIPTIVE_STATS: descriptive_stats
    }
    return desc_stats_json_dict


def _structure_cluster_analysis_json(cluster_analysis_dict):
    """
    Converts the dict/json returned by spark cluster analysis into the correct format that the backend
    expects in the REST call

    Args:
        :cluster_analysis_dict: the raw data

    Returns:
        the formatted data

    """
    data_points = []
    clusters = []
    for key, value in cluster_analysis_dict[constants.FEATURE_STORE.CLUSTERING_ANALYSIS_FEATURES_COLUMN].items():
        try:
            first_dim = float(value[constants.FEATURE_STORE.CLUSTERING_ANALYSIS_ARRAY_COLUMN][0])
            second_dim = float(value[constants.FEATURE_STORE.CLUSTERING_ANALYSIS_ARRAY_COLUMN][1])
            if math.isnan(first_dim):
                first_dim = 0.0
            if math.isnan(second_dim):
                second_dim = 0.0
        except ValueError:
            first_dim = 0.0
            second_dim = 0.0
        data_point = {
            constants.REST_CONFIG.JSON_CLUSTERING_ANALYSIS_DATA_POINT_NAME: str(key),
            constants.REST_CONFIG.JSON_CLUSTERING_ANALYSIS_FIRST_DIMENSION: first_dim,
            constants.REST_CONFIG.JSON_CLUSTERING_ANALYSIS_SECOND_DIMENSION: second_dim,
        }
        data_points.append(data_point)
    for key, value in cluster_analysis_dict[constants.FEATURE_STORE.CLUSTERING_ANALYSIS_CLUSTERS_OUTPUT_COLUMN].items():
        try:
            cluster_val = int(value)
            if math.isnan(cluster_val):
                cluster_val = -1
        except ValueError:
            cluster_val = -1
        cluster = {
            constants.REST_CONFIG.JSON_CLUSTERING_ANALYSIS_DATA_POINT_NAME: str(key),
            constants.REST_CONFIG.JSON_CLUSTERING_ANALYSIS_CLUSTER: cluster_val
        }
        clusters.append(cluster)
    cluster_analysis_json_dict = {
        constants.REST_CONFIG.JSON_CLUSTERING_ANALYSIS_DATA_POINTS: data_points,
        constants.REST_CONFIG.JSON_CLUSTERING_ANALYSIS_CLUSTERS: clusters
    }
    return cluster_analysis_json_dict


def _structure_feature_histograms_json(feature_histogram_list):
    """
    Converts the dict/json returned by spark histogram computation into the correct format that the backend
    expects in the REST call

    Args:
        :feature_histogram_list: the raw data

    Returns:
        the formatted data

    """
    feature_distributions = []
    for dist in feature_histogram_list:
        frequency_distribution = []
        for bin, freq in dist[constants.FEATURE_STORE.HISTOGRAM_FREQUENCY].items():
            try:
                freq_val = int(freq)
                if math.isnan(freq_val):
                    freq_val = 0
            except ValueError:
                freq_val = 0
            histogram_bin = {
                constants.REST_CONFIG.JSON_HISTOGRAM_BIN: str(bin),
                constants.REST_CONFIG.JSON_HISTOGRAM_FREQUENCY: freq_val
            }
            frequency_distribution.append(histogram_bin)
        feature_distribution = {
            constants.REST_CONFIG.JSON_HISTOGRAM_FEATURE_NAME: dist[constants.FEATURE_STORE.HISTOGRAM_FEATURE],
            constants.REST_CONFIG.JSON_HISTOGRAM_FREQUENCY_DISTRIBUTION: frequency_distribution
        }
        feature_distributions.append(feature_distribution)
    feature_distributions_dict = {
        constants.REST_CONFIG.JSON_HISTOGRAM_FEATURE_DISTRIBUTIONS: feature_distributions
    }
    return feature_distributions_dict


def _structure_feature_corr_json(feature_corr_dict):
    """
    Converts the dict/json returned by spark correlation analysis into the correct format that the backend
    expects in the REST call

    Args:
        :feature_corr_dict: the raw data

    Returns:
        the formatted data
    """
    feature_correlations = []
    for key, value in feature_corr_dict.items():
        correlation_values = []
        for key1, value1 in value.items():
            try:
                corr = float(value1)
                if math.isnan(corr):
                    corr = 0.0
            except ValueError:
                corr = 0.0
            correlation_value = {
                constants.REST_CONFIG.JSON_CORRELATION_FEATURE_NAME: str(key1),
                constants.REST_CONFIG.JSON_CORRELATION: corr
            }
            correlation_values.append(correlation_value)
        feature_correlation = {
            constants.REST_CONFIG.JSON_CORRELATION_FEATURE_NAME: str(key),
            constants.REST_CONFIG.JSON_CORRELATION_VALUES: correlation_values
        }
        feature_correlations.append(feature_correlation)
    correlation_matrix_dict = {
        constants.REST_CONFIG.JSON_FEATURE_CORRELATIONS: feature_correlations
    }
    return correlation_matrix_dict


def _create_featuregroup_rest(featuregroup, featurestore, description, featuregroup_version, job_name, dependencies,
                              features_schema, feature_corr_data, featuregroup_desc_stats_data,
                              features_histogram_data, cluster_analysis_data):
    """
    Sends a REST call to hopsworks to create a new featuregroup with specified metadata

    Args:
        :featuregroup: the name of the featuregroup
        :featurestore: the featurestore of the featuregroup (defaults to the project's featurestore)
        :description:  a description of the featuregroup
        :featuregroup_version: the version of the featuregroup (defaults to 1)
        :job_name: the name of the job to compute the featuregroup
        :dependencies: list of the datasets that this featuregroup depends on (e.g input datasets to the feature engineering job)
        :features_schema: the schema of the featuregroup
        :feature_corr_data: json-string with the feature correlation matrix of the featuregroup
        :featuregroup_desc_stats_data: json-string with the descriptive statistics of the featurergroup
        :features_histogram_data: list of json-strings with histogram data for the features in the featuregroup
        :cluster_analysis_data: cluster analysis for the featuregroup

    Returns:
        The HTTP response

    """
    json_contents = tls._prepare_rest_appservice_json_request()
    json_contents[constants.REST_CONFIG.JSON_FEATURESTORENAME] = featurestore
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUPNAME] = featuregroup
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION] = featuregroup_version
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_DESCRIPTION] = description
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_JOBNAME] = job_name
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_DEPENDENCIES] = dependencies
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES] = features_schema
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURE_CORRELATION] = feature_corr_data
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_DESC_STATS] = featuregroup_desc_stats_data
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES_HISTOGRAM] = features_histogram_data
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES_CLUSTERS] = cluster_analysis_data
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_UPDATE_METADATA] = False
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_UPDATE_STATS] = False
    json_embeddable = json.dumps(json_contents)
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method = constants.HTTP_CONFIG.HTTP_POST
    connection = util._get_http_connection(https=True)
    resource = constants.REST_CONFIG.HOPSWORKS_CREATE_FEATUREGROUP_RESOURCE
    resource_url = constants.DELIMITERS.SLASH_DELIMITER + constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + constants.REST_CONFIG.HOPSWORKS_REST_APPSERVICE + constants.DELIMITERS.SLASH_DELIMITER + resource
    connection.request(method, resource_url, json_embeddable, headers)
    response = connection.getresponse()
    resp_body = response.read()
    response_object = json.loads(resp_body)
    try:  # for python 3
        if response.code != 201 and response.code != 200:
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise AssertionError("Could not create feature group, server response: \n " \
                                 "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                response.code, response.reason, error_code, error_msg, user_msg))
    except:  # for python 2
        if response.status != 201 and response.status != 200:
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise AssertionError("Could not create feature group, server response: \n " \
                                 "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                response.status, response.reason, error_code, error_msg, user_msg))
    return response_object


def _update_featuregroup_stats_rest(featuregroup, featurestore, featuregroup_version, feature_corr,
                                    featuregroup_desc_stats_data, features_histogram_data, cluster_analysis_data):
    """
    Makes a REST call to hopsworks appservice for updating the statistics of a particular featuregroup

    Args:
        :featuregroup: the featuregroup to update statistics for
        :featurestore: the featurestore where the featuregroup resides
        :featuregroup_version: the version of the featuregroup
        :feature_corr: the feature correlation matrix
        :featuregroup_desc_stats_data: the descriptive statistics of the featuregroup
        :features_histogram_data: the histograms of the features in the featuregroup
        :cluster_analysis_data: the clusters from cluster analysis on the featuregroup

    Returns:
        The REST response
    """
    json_contents = tls._prepare_rest_appservice_json_request()
    json_contents[constants.REST_CONFIG.JSON_FEATURESTORENAME] = featurestore
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUPNAME] = featuregroup
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION] = featuregroup_version
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_JOBNAME] = None
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_DEPENDENCIES] = []
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_UPDATE_METADATA] = False
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_UPDATE_STATS] = True
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURE_CORRELATION] = feature_corr
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_DESC_STATS] = featuregroup_desc_stats_data
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES_HISTOGRAM] = features_histogram_data
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES_CLUSTERS] = cluster_analysis_data
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES] = []
    json_embeddable = json.dumps(json_contents, allow_nan=False)
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method = constants.HTTP_CONFIG.HTTP_PUT
    connection = util._get_http_connection(https=True)
    resource = constants.REST_CONFIG.HOPSWORKS_UPDATE_FEATUREGROUP_METADATA
    resource_url = constants.DELIMITERS.SLASH_DELIMITER + constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + constants.REST_CONFIG.HOPSWORKS_REST_APPSERVICE + constants.DELIMITERS.SLASH_DELIMITER + resource
    connection.request(method, resource_url, json_embeddable, headers)
    response = connection.getresponse()
    resp_body = response.read()
    response_object = json.loads(resp_body)
    try:  # for python 3
        if (response.code != 200):
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise AssertionError("Could not update featuregroup stats, server response: \n " \
                                 "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                response.code, response.reason, error_code, error_msg, user_msg))
    except:  # for python 2
        if (response.status != 200):
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise AssertionError("Could not update featuregroup stats, server response: \n " \
                                 "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                response.status, response.reason, error_code, error_msg, user_msg))
    return response_object


def update_featuregroup_stats(featuregroup, featuregroup_version=1, featurestore=None, descriptive_statistics=True,
                              feature_correlation=True, feature_histograms=True, cluster_analysis=True,
                              stat_columns=None, num_bins=20,
                              num_clusters=5, corr_method='pearson'):
    """
    Updates the statistics of a featuregroup by computing the statistics with spark and then saving it to Hopsworks by
    making a REST call.

    Example usage:

    >>> # The API will default to the project's featurestore, featuregroup version 1, and compute all statistics for all columns
    >>> featurestore.update_featuregroup_stats("trx_summary_features")
    >>> # You can also be explicitly specify featuregroup details and what statistics to compute:
    >>> featurestore.update_featuregroup_stats("trx_summary_features", featuregroup_version=1, featurestore=featurestore.project_featurestore(), descriptive_statistics=True,feature_correlation=True, feature_histograms=True, cluster_analysis=True, stat_columns=None)
    >>> # If you only want to compute statistics for certain set of columns and exclude surrogate key-columns for example, you can use the optional argument stat_columns to specify which columns to include:
    >>> featurestore.update_featuregroup_stats("trx_summary_features", featuregroup_version=1, featurestore=featurestore.project_featurestore(), descriptive_statistics=True, feature_correlation=True, feature_histograms=True, cluster_analysis=True, stat_columns=['avg_trx', 'count_trx', 'max_trx', 'min_trx'])

    Args:
        :featuregroup: the featuregroup to update the statistics for
        :featuregroup_version: the version of the featuregroup (defaults to 1)
        :featurestore: the featurestore where the featuregroup resides (defaults to the project's featurestore)
        :descriptive_statistics: a boolean flag whether to compute descriptive statistics (min,max,mean etc) for the featuregroup
        :feature_correlation: a boolean flag whether to compute a feature correlation matrix for the numeric columns in the featuregroup
        :feature_histograms: a boolean flag whether to compute histograms for the numeric columns in the featuregroup
        :cluster_analysis: a boolean flag whether to compute cluster analysis for the numeric columns in the featuregroup
        :stat_columns: a list of columns to compute statistics for (defaults to all columns that are numeric)
        :num_bins: number of bins to use for computing histograms
        :num_clusters: the number of clusters to use in clustering analysis (k-means)
        :corr_method: the method to compute feature correlation with (pearson or spearman)

    Returns:
        None
    """
    if featurestore is None:
        featurestore = project_featurestore()
    feature_corr_data, featuregroup_desc_stats_data, features_histogram_data, cluster_analysis_data = _compute_dataframe_stats(
        featuregroup, version=featuregroup_version, featurestore=featurestore,
        descriptive_statistics=descriptive_statistics, feature_correlation=feature_correlation,
        feature_histograms=feature_histograms, cluster_analysis=cluster_analysis, stat_columns=stat_columns,
        num_bins=num_bins, corr_method=corr_method,
        num_clusters=num_clusters)
    _update_featuregroup_stats_rest(featuregroup, featurestore, featuregroup_version, feature_corr_data,
                                    featuregroup_desc_stats_data, features_histogram_data, cluster_analysis_data)


def _get_default_primary_key(featuregroup_df):
    """
    Gets the default primary key of a featuregroup (the first column)

    Args:
        :featuregroup_df: the featuregroup to get the primary key for

    Returns:
        the name of the first column in the featuregroup

    """
    return featuregroup_df.dtypes[0][0]


def _validate_primary_key(featuregroup_df, primary_key):
    """
    Validates a user-supplied primary key

    Args:
        :featuregroup_df: the featuregroup_df that should contain the primary key
        :primary_key: the name of the primary key

    Returns:
        True if the validation succeeded, otherwise raises an error

    """
    cols = map(lambda x: x[0], featuregroup_df.dtypes)
    if primary_key in cols:
        return True
    else:
        raise AssertionError(
            "Invalid primary key: {}, the specified primary key does not exists among the available columns: {}".format(
                cols))


def create_featuregroup(df, featuregroup, primary_key=None, description="", featurestore=None,
                        featuregroup_version=1, job_name=None,
                        dependencies=[], descriptive_statistics=True, feature_correlation=True,
                        feature_histograms=True, cluster_analysis=True, stat_columns=None, num_bins=20,
                        corr_method='pearson',
                        num_clusters=5):
    """
    Creates a new featuregroup from a dataframe of features (sends the metadata to Hopsworks with a REST call to create the
    Hive table and store the metadata and then inserts the data of the spark dataframe into the newly created table)

    Example usage:

    >>> # By default the new featuregroup will be created in the project's featurestore and the statistics for the new featuregroup will be computed based on the provided spark dataframe.
    >>> featurestore.create_featuregroup(trx_summary_df1, "trx_summary_features_2", description="trx_summary_features without the column count_trx")
    >>> # You can also be explicitly specify featuregroup details and what statistics to compute:
    >>> featurestore.create_featuregroup(trx_summary_df1, "trx_summary_features_2_2", description="trx_summary_features without the column count_trx",featurestore=featurestore.project_featurestore(),featuregroup_version=1, job_name=None, dependencies=[], descriptive_statistics=False, feature_correlation=False, feature_histograms=False, cluster_analysis=False, stat_columns=None)

    Args:
        :df: the dataframe to create the featuregroup for (used to infer the schema)
        :featuregroup: the name of the new featuregroup
        :primary_key: the primary key of the new featuregroup, if not specified, the first column in the dataframe will be used as primary
        :description: a description of the featuregroup
        :featurestore: the featurestore of the featuregroup (defaults to the project's featurestore)
        :featuregroup_version: the version of the featuregroup (defaults to 1)
        :job_name: the name of the job to compute the featuregroup
        :dependencies: list of the datasets that this featuregroup depends on (e.g input datasets to the feature engineering job)
        :descriptive_statistics: a boolean flag whether to compute descriptive statistics (min,max,mean etc) for the featuregroup
        :feature_correlation: a boolean flag whether to compute a feature correlation matrix for the numeric columns in the featuregroup
        :feature_histograms: a boolean flag whether to compute histograms for the numeric columns in the featuregroup
        :cluster_analysis: a boolean flag whether to compute cluster analysis for the numeric columns in the featuregroup
        :stat_columns: a list of columns to compute statistics for (defaults to all columns that are numeric)
        :num_bins: number of bins to use for computing histograms
        :num_clusters: the number of clusters to use for cluster analysis
        :corr_method: the method to compute feature correlation with (pearson or spearman)

    Returns:
        None

    """
    try:
        spark_df = _convert_dataframe_to_spark(df)
    except Exception as e:
        raise AssertionError("Could not convert the provided dataframe to a spark dataframe which is required in order to save it to the Feature Store, error: {}".format(str(e)))

    _validate_metadata(featuregroup, spark_df.dtypes, dependencies, description)

    if featurestore is None:
        featurestore = project_featurestore()
    if primary_key is None:
        primary_key = _get_default_primary_key(spark_df)
    if job_name is None:
        job_name = util.get_job_name()

    _validate_primary_key(spark_df, primary_key)
    features_schema = _parse_spark_features_schema(spark_df.schema, primary_key)
    feature_corr_data, featuregroup_desc_stats_data, features_histogram_data, cluster_analysis_data = \
        _compute_dataframe_stats(
            featuregroup, spark_df=spark_df, version=featuregroup_version, featurestore=featurestore,
            descriptive_statistics=descriptive_statistics, feature_correlation=feature_correlation,
            feature_histograms=feature_histograms, cluster_analysis=cluster_analysis, stat_columns=stat_columns,
            num_bins=num_bins,
            corr_method=corr_method,
            num_clusters=num_clusters)
    _create_featuregroup_rest(featuregroup, featurestore, description, featuregroup_version, job_name,
                              dependencies, features_schema,
                              feature_corr_data, featuregroup_desc_stats_data, features_histogram_data,
                              cluster_analysis_data)
    _write_featuregroup_hive(spark_df, featuregroup, featurestore, featuregroup_version, constants.FEATURE_STORE.FEATURE_GROUP_INSERT_APPEND_MODE)
    #update metadata cache
    _get_featurestore_metadata(featurestore, update_cache=True)
    _log("Feature group created successfully")


def get_featurestore_metadata(featurestore=None):
    """
    Sends a REST call to Hopsworks to get the list of featuregroups and their features for the given featurestore.

    Example usage:

    >>> # The API will default to the project's feature store
    >>> featurestore.get_featurestore_metadata()
    >>> # You can also explicitly define the feature store
    >>> featurestore.get_featurestore_metadata(featurestore=featurestore.project_featurestore())

    Args:
        :featurestore: the featurestore to query metadata of

    Returns:
        A list of featuregroups and their metadata

    """
    if featurestore is None:
        featurestore = project_featurestore()
    return _get_feature_store_metadata(featurestore)


def get_featuregroups(featurestore=None):
    """
    Gets a list of all featuregroups in a featurestore

    >>> # List all Feature Groups in a Feature Store
    >>> featurestore.get_featuregroups()
    >>> # By default `get_featuregroups()` will use the project's feature store, but this can also be specified with the optional argument `featurestore`
    >>> featurestore.get_featuregroups(featurestore=featurestore.project_featurestore())

    Args:
        :featurestore: the featurestore to list featuregroups for, defaults to the project-featurestore

    Returns:
        A list of names of the featuregroups in this featurestore
    """
    if featurestore is None:
        featurestore = project_featurestore()
    featurestore_metadata = _get_feature_store_metadata(featurestore)
    featuregroup_names = list(map(lambda fg: _get_table_name(fg[constants.REST_CONFIG.JSON_FEATUREGROUPNAME],
                                                             fg[constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION]),
                                  featurestore_metadata[constants.REST_CONFIG.JSON_FEATUREGROUPS]))
    return featuregroup_names

def get_features_list(featurestore=None):
    """
    Gets a list of all features in a featurestore

    >>> # List all Features in a Feature Store
    >>> featurestore.get_features_list()
    >>> # By default `get_features_list()` will use the project's feature store, but this can also be specified with the optional argument `featurestore`
    >>> featurestore.get_features_list(featurestore=featurestore.project_featurestore())

    Args:
        :featurestore: the featurestore to list features for, defaults to the project-featurestore

    Returns:
        A list of names of the features in this featurestore
    """
    if featurestore is None:
        featurestore = project_featurestore()
    featurestore_metadata = _get_feature_store_metadata(featurestore)
    features = []
    for fg in featurestore_metadata[constants.REST_CONFIG.JSON_FEATUREGROUPS]:
        features.extend(fg[constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES])
    features = list(map(lambda f: f[constants.REST_CONFIG.JSON_FEATURE_NAME], features))
    return features

def get_training_datasets(featurestore=None):
    """
    Gets a list of all training datasets in a featurestore

    >>> # List all Training Datasets in a Feature Store
    >>> featurestore.get_training_datasets()
    >>> # By default `get_training_datasets()` will use the project's feature store, but this can also be specified with the optional argument featurestore
    >>> featurestore.get_training_datasets(featurestore=featurestore.project_featurestore())

    Args:
        :featurestore: the featurestore to list training datasets for, defaults to the project-featurestore

    Returns:
        A list of names of the training datasets in this featurestore
    """
    if featurestore is None:
        featurestore = project_featurestore()
    featurestore_metadata = _get_feature_store_metadata(featurestore)
    training_dataset_names = list(map(lambda td: _get_table_name(td[constants.REST_CONFIG.JSON_TRAINING_DATASET_NAME],
                                                                 td[
                                                                     constants.REST_CONFIG.JSON_TRAINING_DATASET_VERSION]),
                                      featurestore_metadata[constants.REST_CONFIG.JSON_TRAINING_DATASETS]))
    return training_dataset_names


def get_project_featurestores():
    """
    Gets all featurestores for the current project

    Example usage:

    >>> # Get list of featurestores accessible by the current project example
    >>> featurestore.get_project_featurestores()

    Returns:
        A list of all featurestores that the project have access to

    """
    featurestoresJSON = _get_featurestores()
    featurestoreNames = list(map(lambda fsj: fsj[constants.REST_CONFIG.JSON_FEATURESTORENAME], featurestoresJSON))
    return featurestoreNames


def get_dataframe_tf_record_schema(spark_df):
    """
    Infers the tf-record schema from a spark dataframe
    Note: this method is just for convenience, it should work in 99% of cases but it is not guaranteed,
    if spark or tensorflow introduces new datatypes this will break. The user can allways fallback to encoding the
    tf-example-schema manually.

    Args:
        :spark_df: the spark dataframe to infer the tensorflow example record from

    Returns:
        a dict with the tensorflow example
    """
    return _get_dataframe_tf_record_schema_json(spark_df)[0]

def _get_spark_array_size(spark_df, array_col_name):
    """
    Gets the size of an array column in the dataframe

    Args:
        :spark_df: the spark dataframe that contains the array column
        :array_col_name: the name of the array column in the spark dataframe

    Returns:
        The length of the the array column (assuming fixed size)
    """
    return len(getattr(spark_df.select(array_col_name).first(), array_col_name))


def _get_dataframe_tf_record_schema_json(spark_df, fixed=True):
    """
    Infers the tf-record schema from a spark dataframe
    Note: this method is just for convenience, it should work in 99% of cases but it is not guaranteed,
    if spark or tensorflow introduces new datatypes this will break. The user can always fallback to encoding the
    tf-example-schema manually.

    Can only handle one level of nesting, e.g arrays inside the dataframe is okay but having a schema of
     array<array<float>> will not work.

    Args:
        :spark_df: the spark dataframe to infer the tensorflow example record from
        :fixed: boolean flag indicating whether array columns should be treated with fixed size or variable size

    Returns:
        a dict with the tensorflow example as well as a json friendly version of the schema
    """
    example = {}
    example_json = {}
    for col in spark_df.dtypes:
        if col[1] in constants.FEATURE_STORE.TF_RECORD_INT_SPARK_TYPES:
            example[str(col[0])] = tf.FixedLenFeature([], tf.int64)
            example_json[str(col[0])] = {
                constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE: constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_FIXED,
                constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE: constants.FEATURE_STORE.TF_RECORD_INT_TYPE}
            tf.FixedLenFeature([], tf.int64)
        if col[1] in constants.FEATURE_STORE.TF_RECORD_FLOAT_SPARK_TYPES:
            example[str(col[0])] = tf.FixedLenFeature([], tf.float32)
            example_json[str(col[0])] = {
                constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE: constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_FIXED,
                constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE: constants.FEATURE_STORE.TF_RECORD_FLOAT_TYPE}
        if col[1] in constants.FEATURE_STORE.TF_RECORD_INT_ARRAY_SPARK_TYPES:
            if fixed:
                array_len = _get_spark_array_size(spark_df, str(col[0]))
                example[str(col[0])] = tf.FixedLenFeature(shape=[array_len], dtype=tf.int64)
                example_json[str(col[0])] = {
                    constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE: constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_FIXED,
                    constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE: constants.FEATURE_STORE.TF_RECORD_INT_TYPE,
                    constants.FEATURE_STORE.TF_RECORD_SCHEMA_SHAPE: [array_len]
                }
            else:
                example[str(col[0])] = tf.VarLenFeature(tf.int64)
                example_json[str(col[0])] = {
                    constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE: constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_VAR,
                    constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE: constants.FEATURE_STORE.TF_RECORD_INT_TYPE}
        if col[1] in constants.FEATURE_STORE.TF_RECORD_FLOAT_ARRAY_SPARK_TYPES:
            if fixed:
                array_len = _get_spark_array_size(spark_df, str(col[0]))
                example[str(col[0])] = tf.FixedLenFeature(shape=[array_len], dtype=tf.float32)
                example_json[str(col[0])] = {
                    constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE: constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_FIXED,
                    constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE: constants.FEATURE_STORE.TF_RECORD_FLOAT_TYPE,
                    constants.FEATURE_STORE.TF_RECORD_SCHEMA_SHAPE: [array_len]
                }
            else:
                example[str(col[0])] = tf.VarLenFeature(tf.float32)
                example_json[str(col[0])] = {
                    constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE: constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_VAR,
                    constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE: constants.FEATURE_STORE.TF_RECORD_FLOAT_TYPE}
        if col[1] in constants.FEATURE_STORE.TF_RECORD_STRING_ARRAY_SPARK_TYPES or col[1] in constants.FEATURE_STORE.TF_RECORD_STRING_SPARK_TYPES:
            example[str(col[0])] = tf.VarLenFeature(tf.string)
            example_json[str(col[0])] = {
                constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE: constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_VAR,
                constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE: constants.FEATURE_STORE.TF_RECORD_STRING_TYPE}

        recognized_tf_record_types = [constants.SPARK_CONFIG.SPARK_VECTOR, constants.SPARK_CONFIG.SPARK_ARRAY_BINARY,
                            constants.SPARK_CONFIG.SPARK_ARRAY_STRING, constants.SPARK_CONFIG.SPARK_ARRAY_DECIMAL,
                            constants.SPARK_CONFIG.SPARK_ARRAY_DOUBLE, constants.SPARK_CONFIG.SPARK_ARRAY_FLOAT,
                            constants.SPARK_CONFIG.SPARK_ARRAY_LONG, constants.SPARK_CONFIG.SPARK_ARRAY_INTEGER,
                            constants.SPARK_CONFIG.SPARK_BINARY_TYPE, constants.SPARK_CONFIG.SPARK_STRING_TYPE,
                            constants.SPARK_CONFIG.SPARK_DECIMAL_TYPE, constants.SPARK_CONFIG.SPARK_DOUBLE_TYPE,
                            constants.SPARK_CONFIG.SPARK_FLOAT_TYPE, constants.SPARK_CONFIG.SPARK_LONG_TYPE,
                            constants.SPARK_CONFIG.SPARK_INT_TYPE, constants.SPARK_CONFIG.SPARK_INTEGER_TYPE,
                            constants.SPARK_CONFIG.SPARK_ARRAY_BIGINT, constants.SPARK_CONFIG.SPARK_BIGINT_TYPE]
        if col[1] not in recognized_tf_record_types:
            raise AssertionError("Could not recognize the spark type: {} for inferring the tf-records schema."
                                 "Recognized types are: {}".format(col[1], recognized_tf_record_types))
    return example, example_json


def _create_training_dataset_rest(training_dataset, featurestore, description, training_dataset_version,
                                  data_format, job_name, dependencies, features_schema_data,
                                  feature_corr_data, training_dataset_desc_stats_data, features_histogram_data,
                                  cluster_analysis_data):
    """
    Makes a REST request to hopsworks for creating a new training dataset

    Args:
        :training_dataset: the name of the training dataset
        :featurestore: the featurestore that the training dataset is linked to
        :description: a description of the training dataset
        :training_dataset_version: the version of the training dataset (defaults to 1)
        :data_format: the format of the training dataset
        :job_name: the name of the job to compute the training dataset
        :dependencies: list of the datasets that this training dataset depends on (e.g input datasets to the feature engineering job)
        :features_schema_data: the schema of the training dataset
        :feature_corr_data: json-string with the feature correlation matrix of the training dataset
        :cluster_analysis_data: the clusters from cluster analysis on the dataset
        :training_dataset_desc_stats_data: json-string with the descriptive statistics of the training dataset
        :features_histogram_data: list of json-strings with histogram data for the features in the training dataset

    Returns:
        the HTTP response

    """
    json_contents = tls._prepare_rest_appservice_json_request()
    json_contents[constants.REST_CONFIG.JSON_FEATURESTORENAME] = featurestore
    json_contents[constants.REST_CONFIG.JSON_TRAINING_DATASET_NAME] = training_dataset
    json_contents[constants.REST_CONFIG.JSON_TRAINING_DATASET_VERSION] = training_dataset_version
    json_contents[constants.REST_CONFIG.JSON_TRAINING_DATASET_DESCRIPTION] = description
    json_contents[constants.REST_CONFIG.JSON_TRAINING_DATASET_JOBNAME] = job_name
    json_contents[constants.REST_CONFIG.JSON_TRAINING_DATASET_DEPENDENCIES] = dependencies
    json_contents[constants.REST_CONFIG.JSON_TRAINING_DATASET_SCHEMA] = features_schema_data
    json_contents[constants.REST_CONFIG.JSON_TRAINING_DATASET_FEATURE_CORRELATION] = feature_corr_data
    json_contents[constants.REST_CONFIG.JSON_TRAINING_DATASET_DESC_STATS] = training_dataset_desc_stats_data
    json_contents[constants.REST_CONFIG.JSON_TRAINING_DATASET_FEATURES_HISTOGRAM] = features_histogram_data
    json_contents[constants.REST_CONFIG.JSON_TRAINING_DATASET_CLUSTERS] = cluster_analysis_data
    json_contents[constants.REST_CONFIG.JSON_TRAINING_DATASET_FORMAT] = data_format
    json_embeddable = json.dumps(json_contents)
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method = constants.HTTP_CONFIG.HTTP_POST
    connection = util._get_http_connection(https=True)
    resource = constants.REST_CONFIG.HOPSWORKS_CREATE_TRAINING_DATASET_RESOURCE
    resource_url = constants.DELIMITERS.SLASH_DELIMITER + constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + constants.REST_CONFIG.HOPSWORKS_REST_APPSERVICE + constants.DELIMITERS.SLASH_DELIMITER + resource
    connection.request(method, resource_url, json_embeddable, headers)
    response = connection.getresponse()
    resp_body = response.read()
    response_object = json.loads(resp_body)
    try:  # for python 3
        if response.code != 201 and response.code != 200:
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise AssertionError("Could not create training dataset, server response: \n " \
                                 "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                response.code, response.reason, error_code, error_msg, user_msg))
    except:  # for python 2
        if response.status != 201 and response.status != 200:
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise AssertionError("Could not create training dataset, server response: \n " \
                                 "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                response.status, response.reason, error_code, error_msg, user_msg))
    return response_object


def _convert_tf_record_schema_json_to_dict(tf_record_json_schema):
    """
    Converts a JSON version of a tf record schema into a dict that is a valid tf example schema

    Args:
        :tf_record_json_schema: the json version to convert

    Returns:
        the converted schema
    """
    example = {}
    for key, value in tf_record_json_schema.items():
        if value[
            constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE] == constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_FIXED and \
                value[
                    constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE] == constants.FEATURE_STORE.TF_RECORD_INT_TYPE:
            if constants.FEATURE_STORE.TF_RECORD_SCHEMA_SHAPE in value:
                example[str(key)] = tf.FixedLenFeature(shape=value[constants.FEATURE_STORE.TF_RECORD_SCHEMA_SHAPE],
                                                       dtype=tf.int64)
            else:
                example[str(key)] = tf.FixedLenFeature([], tf.int64)
        if value[
            constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE] == constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_FIXED and \
                value[
                    constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE] == constants.FEATURE_STORE.TF_RECORD_FLOAT_TYPE:
            if constants.FEATURE_STORE.TF_RECORD_SCHEMA_SHAPE in value:
                example[str(key)] = tf.FixedLenFeature(shape=value[constants.FEATURE_STORE.TF_RECORD_SCHEMA_SHAPE],
                                                       dtype=tf.float32)
            else:
                example[str(key)] = tf.FixedLenFeature([], tf.float32)
        if value[
            constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE] == constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_VAR and \
                        value[
                            constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE] == constants.FEATURE_STORE.TF_RECORD_INT_TYPE:
            example[str(key)] = tf.VarLenFeature(tf.int64)
        if value[
            constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE] == constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_VAR and \
                        value[
                            constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE] == constants.FEATURE_STORE.TF_RECORD_FLOAT_TYPE:
            example[str(key)] = tf.VarLenFeature(tf.float32)
        if value[
            constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE] == constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_VAR and \
                        value[
                            constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE] == constants.FEATURE_STORE.TF_RECORD_STRING_TYPE:
            example[str(key)] = tf.VarLenFeature(tf.string)
    return example


def _store_tf_record_schema_hdfs(tfrecord_schema, hdfs_path):
    """
    Stores a tfrecord json schema to HDFS

    Args:
        :tfrecord_schema: the tfrecord schema to store
        :hdfs_path: the hdfs path to store it

    Returns:
        None
    """
    json_str = json.dumps(tfrecord_schema)
    hdfs.dump(json_str,
              hdfs_path + constants.DELIMITERS.SLASH_DELIMITER + constants.FEATURE_STORE.TRAINING_DATASET_TF_RECORD_SCHEMA_FILE_NAME)


def get_training_dataset_tf_record_schema(training_dataset, training_dataset_version=1, featurestore=None):
    """
    Gets the tf record schema for a training dataset that is stored in tfrecords format

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
        return _do_get_training_dataset_tf_record_schema(training_dataset, _get_featurestore_metadata(featurestore, update_cache=False), training_dataset_version=training_dataset_version, featurestore=featurestore)
    except:
        return _do_get_training_dataset_tf_record_schema(training_dataset, _get_featurestore_metadata(featurestore, update_cache=True), training_dataset_version=training_dataset_version, featurestore=featurestore)

def _do_get_training_dataset_tf_record_schema(training_dataset, featurestore_metadata, training_dataset_version=1, featurestore=None):
    """
    Gets the tf record schema for a training dataset that is stored in tfrecords format

    Args:
        :training_dataset: the training dataset to get the tfrecords schema for
        :training_dataset_version: the version of the training dataset
        :featurestore_metadata: metadata of the featurestore

    Returns:
        the tf records schema

    """
    training_datasets = featurestore_metadata[constants.REST_CONFIG.JSON_TRAINING_DATASETS]
    training_dataset_json = _find_training_dataset(training_datasets, training_dataset, training_dataset_version)
    if training_dataset_json[
        constants.REST_CONFIG.JSON_TRAINING_DATASET_FORMAT] != constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT:
        raise AssertionError(
            "Cannot fetch tf records schema for a training dataset that is not stored in tfrecords format, this training dataset is stored in format: {}".format(
                training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_FORMAT]))
    hdfs_path = pydoop.path.abspath(training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_HDFS_STORE_PATH])
    tf_record_json_schema = json.loads(hdfs.load(
        hdfs_path + constants.DELIMITERS.SLASH_DELIMITER + constants.FEATURE_STORE.TRAINING_DATASET_TF_RECORD_SCHEMA_FILE_NAME))
    return _convert_tf_record_schema_json_to_dict(tf_record_json_schema)


def get_training_dataset(training_dataset, featurestore=None, training_dataset_version=1, dataframe_type="spark"):
    """
    Reads a training dataset into a spark dataframe

    Args:
        :training_dataset: the name of the training dataset to read
        :featurestore: the featurestore where the training dataset resides
        :training_dataset_version: the version of the training dataset
        :dataframe_type: the type of the returned dataframe (spark, pandas, python or numpy)

    Returns:
        A spark dataframe with the given training dataset data
    """
    if featurestore is None:
        featurestore = project_featurestore()
    spark = util._find_spark()
    training_datasets = _get_feature_store_metadata(featurestore)[constants.REST_CONFIG.JSON_TRAINING_DATASETS]
    training_dataset_json = _find_training_dataset(training_datasets, training_dataset, training_dataset_version)
    hdfs_path = training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_HDFS_STORE_PATH] + \
                constants.DELIMITERS.SLASH_DELIMITER + training_dataset_json[
                    constants.REST_CONFIG.JSON_TRAINING_DATASET_NAME]
    # abspath means "hdfs://namenode:port/ is preprended
    abspath = pydoop.path.abspath(hdfs_path)
    data_format = training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_FORMAT]
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_CSV_FORMAT:
        if hdfs.exists(abspath):
            spark_df = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_CSV_FORMAT).option(
                constants.SPARK_CONFIG.SPARK_WRITE_HEADER, "true").option(
                constants.SPARK_CONFIG.SPARK_WRITE_DELIMITER,
                constants.DELIMITERS.COMMA_DELIMITER).load(abspath)
        elif hdfs.exists(abspath + constants.FEATURE_STORE.TRAINING_DATASET_CSV_SUFFIX):
            spark_df = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_CSV_FORMAT).option(
                constants.SPARK_CONFIG.SPARK_WRITE_HEADER, "true").option(
                constants.SPARK_CONFIG.SPARK_WRITE_DELIMITER,
                constants.DELIMITERS.COMMA_DELIMITER).load(abspath + constants.FEATURE_STORE.TRAINING_DATASET_CSV_SUFFIX)
        if not hdfs.exists(abspath) and not hdfs.exists(abspath + constants.FEATURE_STORE.TRAINING_DATASET_CSV_SUFFIX):
            raise AssertionError("Could not find a training dataset in folder {} or in file {}".format(abspath,
                                                                                                       abspath + constants.FEATURE_STORE.TRAINING_DATASET_CSV_SUFFIX))
        return _return_dataframe_type(spark_df, dataframe_type)
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_TSV_FORMAT:
        if hdfs.exists(abspath):
            spark_df = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_CSV_FORMAT).option(
                constants.SPARK_CONFIG.SPARK_WRITE_HEADER, "true").option(
                constants.SPARK_CONFIG.SPARK_WRITE_DELIMITER,
                constants.DELIMITERS.TAB_DELIMITER).load(abspath)
        elif hdfs.exists(abspath + constants.FEATURE_STORE.TRAINING_DATASET_TSV_SUFFIX):
            spark_df = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_CSV_FORMAT).option(
                constants.SPARK_CONFIG.SPARK_WRITE_HEADER, "true").option(
                constants.SPARK_CONFIG.SPARK_WRITE_DELIMITER,
                constants.DELIMITERS.TAB_DELIMITER).load(abspath + constants.FEATURE_STORE.TRAINING_DATASET_TSV_SUFFIX)
        if not hdfs.exists(abspath) and not hdfs.exists(abspath + constants.FEATURE_STORE.TRAINING_DATASET_TSV_SUFFIX):
            raise AssertionError("Could not find a training dataset in folder {} or in file {}".format(abspath,
                                                                                                       abspath + constants.FEATURE_STORE.TRAINING_DATASET_TSV_SUFFIX))
        return _return_dataframe_type(spark_df, dataframe_type)
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_PARQUET_FORMAT:
        if hdfs.exists(abspath):
            spark_df = spark.read.parquet(abspath)
        elif hdfs.exists(abspath + constants.FEATURE_STORE.TRAINING_DATASET_PARQUET_SUFFIX):
            spark_df = spark.read.parquet(abspath + constants.FEATURE_STORE.TRAINING_DATASET_PARQUET_SUFFIX)
        if not hdfs.exists(abspath) and not hdfs.exists(
                        abspath + constants.FEATURE_STORE.TRAINING_DATASET_PARQUET_SUFFIX):
            raise AssertionError("Could not find a training dataset in folder {} or in file {}".format(abspath,
                                                                                                       abspath + constants.FEATURE_STORE.TRAINING_DATASET_PARQUET_SUFFIX))
        return _return_dataframe_type(spark_df, dataframe_type)
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT:
        if hdfs.exists(abspath):
            spark_df = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT).option(
                constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE,
                constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).load(abspath)
        elif hdfs.exists(abspath + constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_SUFFIX):
            spark_df = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT).option(
                constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE,
                constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).load(abspath + constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_SUFFIX)
        if not hdfs.exists(abspath) and not hdfs.exists(
                        abspath + constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_SUFFIX):
            raise AssertionError("Could not find a training dataset in folder {} or in file {}".format(abspath,
                                                                                                       abspath + constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_SUFFIX))
        return _return_dataframe_type(spark_df, dataframe_type)
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_NPY_FORMAT:
        if not hdfs.exists(abspath + constants.FEATURE_STORE.TRAINING_DATASET_NPY_SUFFIX):
            raise AssertionError("Could not find a training dataset in file {}".format(abspath + constants.FEATURE_STORE.TRAINING_DATASET_NPY_SUFFIX))
        tf = TemporaryFile()
        data = hdfs.load(abspath + constants.FEATURE_STORE.TRAINING_DATASET_NPY_SUFFIX)
        tf.write(data)
        tf.seek(0)
        np_array = np.load(tf)
        if dataframe_type == constants.FEATURE_STORE.DATAFRAME_TYPE_NUMPY:
            return np_array
        if dataframe_type == constants.FEATURE_STORE.DATAFRAME_TYPE_PYTHON:
            return np_array.tolist()
        if dataframe_type == constants.FEATURE_STORE.DATAFRAME_TYPE_SPARK or dataframe_type == constants.FEATURE_STORE.DATAFRAME_TYPE_PANDAS:
            if np_array.ndim != 2:
                raise AssertionError(
                    "Cannot convert numpy array that do not have two dimensions to a dataframe. The number of dimensions are: {}".format(
                        np_array.ndim))
            num_cols = np_array.shape[1]
            dataframe_dict = {}
            for n_col in list(range(num_cols)):
                col_name = "col_" + str(n_col)
                dataframe_dict[col_name] = np_array[:, n_col]
            pandas_df = pd.DataFrame(dataframe_dict)
            sc = spark.sparkContext
            sql_context = SQLContext(sc)
            return _return_dataframe_type(sql_context.createDataFrame(pandas_df), dataframe_type)
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_HDF5_FORMAT:
        if not hdfs.exists(abspath + constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX):
            raise AssertionError("Could not find a training dataset in file {}".format(abspath + constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX))
        tf = TemporaryFile()
        data = hdfs.load(abspath + constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX)
        tf.write(data)
        tf.seek(0)
        hdf5_file = h5py.File(tf)
        np_array = hdf5_file[training_dataset][()]
        if dataframe_type == constants.FEATURE_STORE.DATAFRAME_TYPE_NUMPY:
            return np_array
        if dataframe_type == constants.FEATURE_STORE.DATAFRAME_TYPE_PYTHON:
            return np_array.tolist()
        if dataframe_type == constants.FEATURE_STORE.DATAFRAME_TYPE_SPARK or dataframe_type == constants.FEATURE_STORE.DATAFRAME_TYPE_PANDAS:
            if np_array.ndim != 2:
                raise AssertionError(
                    "Cannot convert numpy array that do not have two dimensions to a dataframe. The number of dimensions are: {}".format(
                        np_array.ndim))
            num_cols = np_array.shape[1]
            dataframe_dict = {}
            for n_col in list(range(num_cols)):
                col_name = "col_" + str(n_col)
                dataframe_dict[col_name] = np_array[:, n_col]
            pandas_df = pd.DataFrame(dataframe_dict)
            sc = spark.sparkContext
            sql_context = SQLContext(sc)
            return _return_dataframe_type(sql_context.createDataFrame(pandas_df), dataframe_type)

    if data_format != constants.FEATURE_STORE.TRAINING_DATASET_TSV_FORMAT and data_format != constants.FEATURE_STORE.TRAINING_DATASET_CSV_FORMAT and data_format != constants.FEATURE_STORE.TRAINING_DATASET_PARQUET_FORMAT and data_format != constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT and data_format != constants.FEATURE_STORE.TRAINING_DATASET_NPY_FORMAT and data_format != constants.FEATURE_STORE.TRAINING_DATASET_HDF5_FORMAT:
        raise AssertionError(
            "invalid data format to materialize training dataset. The provided format: {} is not in the list of supported formats: {}".format(
                data_format, ",".join[
                    constants.FEATURE_STORE.TRAINING_DATASET_TSV_FORMAT, constants.FEATURE_STORE.TRAINING_DATASET_CSV_FORMAT, constants.FEATURE_STORE.TRAINING_DATASET_PARQUET_FORMAT, constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT, constants.FEATURE_STORE.TRAINING_DATASET_NPY_FORMAT, constants.FEATURE_STORE.TRAINING_DATASET_HDF5_FORMAT]))


def _write_training_dataset_hdfs(df, path, data_format, write_mode, name):
    """
    Materializes a spark dataframe to a training dataset on HDFS.

    Args:
        :df: the dataframe to materialize
        :path: the hdfs path where the dataframe will be materialized
        :data_format: the format to materialize to
        :write_mode: spark write mode, 'append' or 'overwrite'
        :name: the name of the training dataset

    Returns:
        None
    """
    spark = util._find_spark()
    spark.sparkContext.setJobGroup("Materializing dataframe as training dataset",
                                   "Saving training dataset in path: {} in format {}".format(path, data_format))

    # some spark versions cannot handle overwrite, so then this is necessary
    # if(hdfs.exists(path)):
    #   hdfs.rmr(path)
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_CSV_FORMAT:
        df.write.option(constants.SPARK_CONFIG.SPARK_WRITE_DELIMITER, constants.DELIMITERS.COMMA_DELIMITER).mode(
            write_mode).option(
            constants.SPARK_CONFIG.SPARK_WRITE_HEADER, "true").csv(path)
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_TSV_FORMAT:
        df.write.option(constants.SPARK_CONFIG.SPARK_WRITE_DELIMITER, constants.DELIMITERS.TAB_DELIMITER).mode(
            write_mode).option(
            constants.SPARK_CONFIG.SPARK_WRITE_HEADER, "true").csv(path)
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_PARQUET_FORMAT:
        df.write.mode(write_mode).parquet(path)
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT:
        if (write_mode == constants.SPARK_CONFIG.SPARK_APPEND_MODE):
            raise AssertionError(
                "Append is not supported for training datasets stored in tf-records format, only overwrite, set the optional argument write_mode='overwrite'")
        df.write.format(data_format).option(constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE,
                                            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).mode(
            write_mode).save(path)
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_NPY_FORMAT:
        if (write_mode == constants.SPARK_CONFIG.SPARK_APPEND_MODE):
            raise AssertionError(
                "Append is not supported for training datasets stored in .npy format, only overwrite, set the optional argument write_mode='overwrite'")
        if not isinstance(df, np.ndarray):
            if isinstance(df, DataFrame) or isinstance(df, RDD):
                df = np.array(df.collect())
            if isinstance(df, pd.DataFrame):
                df = df.values
            if isinstance(df, list):
                df = np.array(df)
        tf = TemporaryFile()
        tf.seek(0)
        np.save(tf, df)
        tf.seek(0)
        hdfs.dump(tf.read(), path + constants.FEATURE_STORE.TRAINING_DATASET_NPY_SUFFIX)
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_HDF5_FORMAT:
        if (write_mode == constants.SPARK_CONFIG.SPARK_APPEND_MODE):
            raise AssertionError(
                "Append is not supported for training datasets stored in .hdf5 format, only overwrite, set the optional argument write_mode='overwrite'")
        if not isinstance(df, np.ndarray):
            if isinstance(df, DataFrame) or isinstance(df, RDD):
                df = np.array(df.collect())
            if isinstance(df, pd.DataFrame):
                df = df.values
            if isinstance(df, list):
                df = np.array(df)
        tf = TemporaryFile()
        tf.seek(0)
        hdf5_file = h5py.File(tf)
        tf.seek(0)
        hdf5_file.create_dataset(name, data=df)
        tf.seek(0)
        hdf5_file.close()
        tf.seek(0)
        hdfs.dump(tf.read(), path + constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX)

    if data_format != constants.FEATURE_STORE.TRAINING_DATASET_TSV_FORMAT and data_format != constants.FEATURE_STORE.TRAINING_DATASET_CSV_FORMAT and data_format != constants.FEATURE_STORE.TRAINING_DATASET_PARQUET_FORMAT and data_format != constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT and data_format != constants.FEATURE_STORE.TRAINING_DATASET_NPY_FORMAT and data_format != constants.FEATURE_STORE.TRAINING_DATASET_HDF5_FORMAT:
        raise AssertionError(
            "invalid data format to materialize training dataset. The provided format: {} is not in the list of supported formats: {}".format(
                data_format, ",".join[
                    constants.FEATURE_STORE.TRAINING_DATASET_TSV_FORMAT, constants.FEATURE_STORE.TRAINING_DATASET_CSV_FORMAT, constants.FEATURE_STORE.TRAINING_DATASET_PARQUET_FORMAT, constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT, constants.FEATURE_STORE.TRAINING_DATASET_NPY_FORMAT, constants.FEATURE_STORE.TRAINING_DATASET_HDF5_FORMAT]))

    spark.sparkContext.setJobGroup("", "")


def create_training_dataset(df, training_dataset, description="", featurestore=None,
                            data_format="tfrecords", training_dataset_version=1,
                            job_name=None, dependencies=[], descriptive_statistics=True, feature_correlation=True,
                            feature_histograms=True, cluster_analysis=True, stat_columns=None, num_bins=20,
                            corr_method='pearson',
                            num_clusters=5):
    """
    Creates a new training dataset from a dataframe, saves metadata about the training dataset to the database
    and saves the materialized dataset on hdfs

    Example usage:

    >>> featurestore.create_training_dataset(dataset_df, "AML_dataset")
    >>> # You can override the default configuration if necessary:
    >>> featurestore.create_training_dataset(dataset_df, "TestDataset", description="", featurestore=featurestore.project_featurestore(), data_format="csv", training_dataset_version=1, job_name=None, dependencies=[], descriptive_statistics=False, feature_correlation=False, feature_histograms=False, cluster_analysis=False, stat_columns=None)

    Args:
        :df: the dataframe to create the training dataset from
        :training_dataset: the name of the training dataset
        :description: a description of the training dataset
        :featurestore: the featurestore that the training dataset is linked to
        :data_format: the format of the materialized training dataset
        :training_dataset_version: the version of the training dataset (defaults to 1)
        :job_name: the name of the job to compute the training dataset
        :dependencies: list of the datasets that this training dataset depends on (e.g input datasets to the feature engineering job)
        :descriptive_statistics: a boolean flag whether to compute descriptive statistics (min,max,mean etc) for the featuregroup
        :feature_correlation: a boolean flag whether to compute a feature correlation matrix for the numeric columns in the featuregroup
        :feature_histograms: a boolean flag whether to compute histograms for the numeric columns in the featuregroup
        :cluster_analysis: a boolean flag whether to compute cluster analysis for the numeric columns in the featuregroup
        :stat_columns: a list of columns to compute statistics for (defaults to all columns that are numeric)
        :num_bins: number of bins to use for computing histograms
        :num_clusters: number of clusters to use for cluster analysis
        :corr_method: the method to compute feature correlation with (pearson or spearman)

    Returns:
        None
    """
    try:
        spark_df = _convert_dataframe_to_spark(df)
    except Exception as e:
        raise AssertionError("Could not convert the provided dataframe to a spark dataframe which is required in order to save it to the Feature Store, error: {}".format(str(e)))

    _validate_metadata(training_dataset, spark_df.dtypes, dependencies, description)

    if featurestore is None:
        featurestore = project_featurestore()
    if job_name is None:
        job_name = util.get_job_name()

    feature_corr_data, training_dataset_desc_stats_data, features_histogram_data, cluster_analysis_data = \
        _compute_dataframe_stats(
            training_dataset, spark_df=spark_df, version=training_dataset_version, featurestore=featurestore,
            descriptive_statistics=descriptive_statistics, feature_correlation=feature_correlation,
            feature_histograms=feature_histograms, cluster_analysis=cluster_analysis, stat_columns=stat_columns,
            num_bins=num_bins,
            corr_method=corr_method,
            num_clusters=num_clusters)
    features_schema = _parse_spark_features_schema(spark_df.schema, None)
    td_json = _create_training_dataset_rest(
        training_dataset, featurestore, description, training_dataset_version,
        data_format, job_name, dependencies, features_schema,
        feature_corr_data, training_dataset_desc_stats_data, features_histogram_data, cluster_analysis_data)
    hdfs_path = pydoop.path.abspath(td_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_HDFS_STORE_PATH])
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT:
        try:
            tf_record_schema_json = _get_dataframe_tf_record_schema_json(spark_df)[1]
            _store_tf_record_schema_hdfs(tf_record_schema_json, hdfs_path)
        except Exception as e:
            _log("Could not infer tfrecords schema for the dataframe, {}".format(str(e)))
    _write_training_dataset_hdfs(spark_df,
                                 hdfs_path + constants.DELIMITERS.SLASH_DELIMITER + training_dataset,
                                 data_format,
                                 constants.SPARK_CONFIG.SPARK_OVERWRITE_MODE,
                                 training_dataset,
                                 petastorm_args)
    #update metadata cache
    _get_featurestore_metadata(featurestore, update_cache=True)
    _log("Training Dataset created successfully")


def _update_training_dataset_stats_rest(
        training_dataset, featurestore, training_dataset_version, features_schema,
        feature_corr_data, featuregroup_desc_stats_data, features_histogram_data, cluster_analysis_data):
    """
    A helper function that makes a REST call to hopsworks for updating the stats and schema metadata about a training dataset

    Args:
        :training_dataset: the name of the training dataset
        :featurestore: the featurestore that the training dataset is linked to
        :training_dataset_version: the version of the training dataset (defaults to 1)
        :features_schema: the schema of the training dataset
        :feature_corr_data:  json-string with the feature correlation matrix of the training dataset
        :featuregroup_desc_stats_data: json-string with the descriptive statistics of the training dataset
        :features_histogram_data: list of json-strings with histogram data for the features in the training dataset
        :cluster_analysis_data: the clusters from cluster analysis on the dataset

    Returns:
        the HTTP response

    """
    json_contents = tls._prepare_rest_appservice_json_request()
    json_contents[constants.REST_CONFIG.JSON_FEATURESTORENAME] = featurestore
    json_contents[constants.REST_CONFIG.JSON_TRAINING_DATASET_NAME] = training_dataset
    json_contents[constants.REST_CONFIG.JSON_TRAINING_DATASET_VERSION] = training_dataset_version
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURE_CORRELATION] = feature_corr_data
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_DESC_STATS] = featuregroup_desc_stats_data
    json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES_HISTOGRAM] = features_histogram_data
    json_contents[constants.REST_CONFIG.JSON_TRAINING_DATASET_CLUSTERS] = cluster_analysis_data
    json_contents[constants.REST_CONFIG.JSON_TRAINING_DATASET_SCHEMA] = features_schema
    json_contents[constants.REST_CONFIG.JSON_TRAINING_DATASET_DEPENDENCIES] = []
    json_contents[constants.REST_CONFIG.JSON_TRAINING_DATASET_UPDATE_METADATA] = False
    json_contents[constants.REST_CONFIG.JSON_TRAINING_DATASET_UPDATE_STATS] = True
    json_embeddable = json.dumps(json_contents)
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method = constants.HTTP_CONFIG.HTTP_PUT
    connection = util._get_http_connection(https=True)
    resource = constants.REST_CONFIG.HOPSWORKS_UPDATE_TRAINING_DATASET_METADATA
    resource_url = constants.DELIMITERS.SLASH_DELIMITER + constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + constants.REST_CONFIG.HOPSWORKS_REST_APPSERVICE + constants.DELIMITERS.SLASH_DELIMITER + resource
    connection.request(method, resource_url, json_embeddable, headers)
    response = connection.getresponse()
    resp_body = response.read()
    response_object = json.loads(resp_body)
    try:  # for python 3
        if (response.code != 200):
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise AssertionError("Could not update training dataset stats, server response: \n " \
                                 "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                response.code, response.reason, error_code, error_msg, user_msg))
    except:  # for python 2
        if (response.status != 200):
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise AssertionError("Could not update training dataset stats, server response: \n " \
                                 "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                response.status, response.reason, error_code, error_msg, user_msg))
    return response_object


def insert_into_training_dataset(
        df, training_dataset, featurestore=None, training_dataset_version=1,
        descriptive_statistics=True, feature_correlation=True,
        feature_histograms=True, cluster_analysis=True, stat_columns=None, num_bins=20, corr_method='pearson',
        num_clusters=5, write_mode="overwrite", ):
    """
    Inserts the data in a training dataset from a spark dataframe (append or overwrite)

    Example usage:

    >>> featurestore.insert_into_training_dataset(dataset_df, "TestDataset")
    >>> # By default the insert_into_training_dataset will use the project's featurestore, version 1,
    >>> # and update the training dataset statistics, this configuration can be overridden:
    >>> featurestore.insert_into_training_dataset(dataset_df,"TestDataset", featurestore=featurestore.project_featurestore(), training_dataset_version=1,descriptive_statistics=True, feature_correlation=True, feature_histograms=True, cluster_analysis=True, stat_columns=None)

    Args:
        :df: the dataframe to write
        :training_dataset: the name of the training dataset
        :featurestore: the featurestore that the training dataset is linked to
        :training_dataset_version: the version of the training dataset (defaults to 1)
        :descriptive_statistics: a boolean flag whether to compute descriptive statistics (min,max,mean etc) for the featuregroup
        :feature_correlation: a boolean flag whether to compute a feature correlation matrix for the numeric columns in the featuregroup
        :feature_histograms: a boolean flag whether to compute histograms for the numeric columns in the featuregroup
        :cluster_analysis: a boolean flag whether to compute cluster analysis for the numeric columns in the featuregroup
        :stat_columns: a list of columns to compute statistics for (defaults to all columns that are numeric)
        :num_bins: number of bins to use for computing histograms
        :num_clusters: number of clusters to use for cluster analysis
        :corr_method: the method to compute feature correlation with (pearson or spearman)
        :write_mode: spark write mode ('append' or 'overwrite'). Note: append is not supported for tfrecords datasets.

    Returns:
        None

    """
    try:
        spark_df = _convert_dataframe_to_spark(df)
    except Exception as e:
        raise AssertionError("Could not convert the provided dataframe to a spark dataframe which is required in order to save it to the Feature Store, error: {}".format(str(e)))

    if featurestore is None:
        featurestore = project_featurestore()
    training_datasets = _get_feature_store_metadata(featurestore)[constants.REST_CONFIG.JSON_TRAINING_DATASETS]
    training_dataset_json = _find_training_dataset(training_datasets, training_dataset, training_dataset_version)
    feature_corr_data, training_dataset_desc_stats_data, features_histogram_data, cluster_analysis_data = _compute_dataframe_stats(
        training_dataset, spark_df=spark_df, version=training_dataset_version, featurestore=featurestore,
        descriptive_statistics=descriptive_statistics, feature_correlation=feature_correlation,
        feature_histograms=feature_histograms, cluster_analysis=cluster_analysis, stat_columns=stat_columns,
        num_bins=num_bins, corr_method=corr_method,
        num_clusters=num_clusters)
    features_schema = _parse_spark_features_schema(spark_df.schema, None)
    td_json = _update_training_dataset_stats_rest(
        training_dataset, featurestore, training_dataset_version,
        features_schema, feature_corr_data, training_dataset_desc_stats_data, features_histogram_data,
        cluster_analysis_data)
    hdfs_path = pydoop.path.abspath(td_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_HDFS_STORE_PATH])
    data_format = training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_FORMAT]
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT:
        try:
            tf_record_schema_json = _get_dataframe_tf_record_schema_json(spark_df)[1]
            _store_tf_record_schema_hdfs(tf_record_schema_json, hdfs_path)
        except Exception as e:
            _log("Could not infer tfrecords schema for the dataframe, {}".format(str(e)))
    _write_training_dataset_hdfs(spark_df,
                                 hdfs_path + constants.DELIMITERS.SLASH_DELIMITER + training_dataset,
                                 data_format,
                                 write_mode,
                                 training_dataset
                                 )


def _find_training_dataset(training_datasets, training_dataset, training_dataset_version):
    """
    A helper function to look for a training dataset name and version in a list of training datasets

    Args:
        :training_datasets: a list of training datasets metadata
        :training_dataset: name of the training dataset
        :training_dataset_version: version of the training dataset

    Returns:
        The training dataset if it finds it, otherwise null
    """
    matches = list(
        filter(lambda td: td[constants.REST_CONFIG.JSON_TRAINING_DATASET_NAME] == training_dataset and
                          td[constants.REST_CONFIG.JSON_TRAINING_DATASET_VERSION] == training_dataset_version,
               training_datasets))
    if len(matches) == 0:
        raise AssertionError("Could not find the requested training dataset with name: {} " \
                             "and version: {} among the list of available training datasets: {}".format(
            training_dataset,
            training_dataset_version,
            training_datasets))
    return matches[0]


def get_training_dataset_path(training_dataset, featurestore=None, training_dataset_version=1):
    """
    Gets the HDFS path to a training dataset with a specific name and version in a featurestore

    Example usage:

    >>> featurestore.get_training_dataset_path("AML_dataset")
    >>> # By default the library will look for the training dataset in the project's featurestore and use version 1, but this can be overriden if required:
    >>> featurestore.get_training_dataset_path("AML_dataset",  featurestore=featurestore.project_featurestore(), training_dataset_version=1)

    Args:
        :training_dataset: name of the training dataset
        :featurestore: featurestore that the training dataset is linked to
        :training_dataset_version: version of the training dataset

    Returns:
        The HDFS path to the training dataset
    """
    if featurestore is None:
        featurestore = project_featurestore()
    training_datasets = _get_feature_store_metadata(featurestore)[constants.REST_CONFIG.JSON_TRAINING_DATASETS]
    training_dataset_json = _find_training_dataset(training_datasets, training_dataset, training_dataset_version)
    hdfs_path = training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_HDFS_STORE_PATH] + \
                constants.DELIMITERS.SLASH_DELIMITER + training_dataset_json[
                    constants.REST_CONFIG.JSON_TRAINING_DATASET_NAME]
    data_format = training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_FORMAT]
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_NPY_FORMAT:
        hdfs_path = hdfs_path + constants.FEATURE_STORE.TRAINING_DATASET_NPY_SUFFIX
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_HDF5_FORMAT:
        hdfs_path = hdfs_path + constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX
    # abspath means "hdfs://namenode:port/ is preprended
    abspath = pydoop.path.abspath(hdfs_path)
    return abspath


def get_latest_training_dataset_version(training_dataset, featurestore=None):
    """
    Utility method to get the latest version of a particular training dataset

    Args:
        :training_dataset: the training dataset to get the latest version of
        :featurestore: the featurestore where the training dataset resides

    Returns:
        the latest version of the training dataset in the feature store
    """
    if featurestore is None:
        featurestore = project_featurestore()

    training_datasets = _get_feature_store_metadata(featurestore)[constants.REST_CONFIG.JSON_TRAINING_DATASETS]
    matches = list(
        filter(lambda x: x[constants.REST_CONFIG.JSON_TRAINING_DATASET_NAME] == training_dataset, training_datasets))
    versions = list(map(lambda x: int(x[constants.REST_CONFIG.JSON_TRAINING_DATASET_VERSION]), matches))
    if (len(versions) > 0):
        return max(versions)
    else:
        return 0;


def get_latest_featuregroup_version(featuregroup, featurestore=None):
    """
    Utility method to get the latest version of a particular featuregroup

    Args:
        :featuregroup: the featuregroup to get the latest version of
        :featurestore: the featurestore where the featuregroup resides

    Returns:
        the latest version of the featuregroup in the feature store
    """
    if featurestore is None:
        featurestore = project_featurestore()

    featuregroups = _get_feature_store_metadata(featurestore)[constants.REST_CONFIG.JSON_FEATUREGROUPS]
    matches = list(filter(lambda x: x[constants.REST_CONFIG.JSON_FEATUREGROUPNAME] == featuregroup, featuregroups))
    versions = list(map(lambda x: int(x[constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION]), matches))
    if (len(versions) > 0):
        return max(versions)
    else:
        return 0


def update_training_dataset_stats(training_dataset, training_dataset_version=1, featurestore=None,
                                  descriptive_statistics=True,
                                  feature_correlation=True, feature_histograms=True, cluster_analysis=True,
                                  stat_columns=None, num_bins=20,
                                  num_clusters=5, corr_method='pearson'):
    """
    Updates the statistics of a featuregroup by computing the statistics with spark and then saving it to Hopsworks by
    making a REST call.

    Example usage:

    >>> # The API will default to the project's featurestore, training dataset version 1, and compute all statistics for all columns
    >>> featurestore.update_training_dataset_stats("teams_prediction")
    >>> # You can also be explicitly specify featuregroup details and what statistics to compute:
    >>> featurestore.update_training_dataset_stats("teams_prediction", training_dataset_version=1, featurestore=featurestore.project_featurestore(), descriptive_statistics=True,feature_correlation=True, feature_histograms=True, cluster_analysis=True, stat_columns=None)
    >>> # If you only want to compute statistics for certain set of columns and exclude surrogate key-columns for example, you can use the optional argument stat_columns to specify which columns to include:
    >>> featurestore.update_training_dataset_stats("teams_prediction", training_dataset_version=1, featurestore=featurestore.project_featurestore(), descriptive_statistics=True, feature_correlation=True, feature_histograms=True, cluster_analysis=True, stat_columns=['avg_trx', 'count_trx', 'max_trx', 'min_trx'])

    Args:
        :training_dataset: the training dataset to update the statistics for
        :training_dataset_version: the version of the training dataset (defaults to 1)
        :featurestore: the featurestore where the training dataset resides (defaults to the project's featurestore)
        :descriptive_statistics: a boolean flag whether to compute descriptive statistics (min,max,mean etc) for the featuregroup
        :feature_correlation: a boolean flag whether to compute a feature correlation matrix for the numeric columns in the featuregroup
        :feature_histograms: a boolean flag whether to compute histograms for the numeric columns in the featuregroup
        :cluster_analysis: a boolean flag whether to compute cluster analysis for the numeric columns in the featuregroup
        :stat_columns: a list of columns to compute statistics for (defaults to all columns that are numeric)
        :num_bins: number of bins to use for computing histograms
        :num_clusters: the number of clusters to use in clustering analysis (k-means)
        :corr_method: the method to compute feature correlation with (pearson or spearman)

    Returns:
        None
    """
    if featurestore is None:
        featurestore = project_featurestore()
    spark_df = get_training_dataset(training_dataset, featurestore=featurestore,
                                    training_dataset_version=training_dataset_version)
    feature_corr_data, training_dataset_desc_stats_data, features_histogram_data, cluster_analysis_data = _compute_dataframe_stats(
        training_dataset, spark_df=spark_df, version=training_dataset_version, featurestore=featurestore,
        descriptive_statistics=descriptive_statistics, feature_correlation=feature_correlation,
        feature_histograms=feature_histograms, cluster_analysis=cluster_analysis, stat_columns=stat_columns,
        num_bins=num_bins, corr_method=corr_method,
        num_clusters=num_clusters)
    features_schema = _parse_spark_features_schema(spark_df.schema, None)
    _update_training_dataset_stats_rest(
        training_dataset, featurestore, training_dataset_version,
        features_schema, feature_corr_data, training_dataset_desc_stats_data, features_histogram_data,
        cluster_analysis_data)
