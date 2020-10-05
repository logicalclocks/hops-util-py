"""
Contains utility functions for operations related to the feature store
"""

from hops import constants, util, hdfs
import json
import numpy as np
from hops.featurestore_impl.exceptions.exceptions import InferTFRecordSchemaError, \
    InvalidPrimaryKey, SparkToHiveSchemaConversionError, CouldNotConvertDataframe, \
    SparkToMySQLSchemaConversionError, FeatureNotFound, FeaturegroupNotFound, \
    TrainingDatasetNotFound
import pandas as pd
import math
import re
import os

# for backwards compatibility
try:
    import h5py
except:
    pass

# in case importing in %%local
try:
    from pyspark.sql import DataFrame, SQLContext
    from pyspark.rdd import RDD
    from pyspark.mllib.stat import Statistics
    from pyspark.ml.feature import VectorAssembler, PCA
    from pyspark.ml.clustering import KMeans
    from pyspark.sql.functions import col
except:
    pass

# for backwards compatibility
try:
    import tensorflow as tf
except:
    pass


def _do_get_latest_training_dataset_version(training_dataset_name, featurestore_metadata):
    """
    Utility method to get the latest version of a particular training dataset

    Args:
        :training_dataset_name: the training dataset to get the latest version of
        :featurestore_metadata: metadata of the featurestore

    Returns:
        the latest version of the training dataset in the feature store
    """
    training_datasets = featurestore_metadata.training_datasets
    matches = list(
        filter(lambda td: td.name == training_dataset_name, training_datasets.values()))
    versions = list(map(lambda td: int(td.version), matches))
    if (len(versions) > 0):
        return max(versions)
    else:
        raise TrainingDatasetNotFound("There was no version for training dataset {} "
                               "found".format(training_dataset_name))


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

    Raises:
        :InferTFRecordSchemaError: if a tf record schema could not be inferred from the dataframe
    """
    example = {}
    example_json = {}
    for col in spark_df.dtypes:
        if col[1] in constants.FEATURE_STORE.TF_RECORD_INT_SPARK_TYPES:
            example[str(col[0])] = tf.io.FixedLenFeature([], tf.int64)
            example_json[str(col[0])] = {
                constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE:
                    constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_FIXED,
                constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE: constants.FEATURE_STORE.TF_RECORD_INT_TYPE}
            tf.io.FixedLenFeature([], tf.int64)
        if col[1] in constants.FEATURE_STORE.TF_RECORD_FLOAT_SPARK_TYPES:
            example[str(col[0])] = tf.io.FixedLenFeature([], tf.float32)
            example_json[str(col[0])] = {
                constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE:
                    constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_FIXED,
                constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE: constants.FEATURE_STORE.TF_RECORD_FLOAT_TYPE}
        if col[1] in constants.FEATURE_STORE.TF_RECORD_INT_ARRAY_SPARK_TYPES:
            if fixed:
                array_len = _get_spark_array_size(spark_df, str(col[0]))
                example[str(col[0])] = tf.io.FixedLenFeature(shape=[array_len], dtype=tf.int64)
                example_json[str(col[0])] = {
                    constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE:
                        constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_FIXED,
                    constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE: constants.FEATURE_STORE.TF_RECORD_INT_TYPE,
                    constants.FEATURE_STORE.TF_RECORD_SCHEMA_SHAPE: [array_len]
                }
            else:
                example[str(col[0])] = tf.io.VarLenFeature(tf.int64)
                example_json[str(col[0])] = {
                    constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE:
                        constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_VAR,
                    constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE: constants.FEATURE_STORE.TF_RECORD_INT_TYPE}
        if col[1] in constants.FEATURE_STORE.TF_RECORD_FLOAT_ARRAY_SPARK_TYPES:
            if fixed:
                array_len = _get_spark_array_size(spark_df, str(col[0]))
                example[str(col[0])] = tf.io.FixedLenFeature(shape=[array_len], dtype=tf.float32)
                example_json[str(col[0])] = {
                    constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE:
                        constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_FIXED,
                    constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE: constants.FEATURE_STORE.TF_RECORD_FLOAT_TYPE,
                    constants.FEATURE_STORE.TF_RECORD_SCHEMA_SHAPE: [array_len]
                }
            else:
                example[str(col[0])] = tf.io.VarLenFeature(tf.float32)
                example_json[str(col[0])] = {
                    constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE:
                        constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_VAR,
                    constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE: constants.FEATURE_STORE.TF_RECORD_FLOAT_TYPE}
        if col[1] in constants.FEATURE_STORE.TF_RECORD_STRING_ARRAY_SPARK_TYPES or col[
            1] in constants.FEATURE_STORE.TF_RECORD_STRING_SPARK_TYPES:
            example[str(col[0])] = tf.io.VarLenFeature(tf.string)
            example_json[str(col[0])] = {
                constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE: constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_VAR,
                constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE: constants.FEATURE_STORE.TF_RECORD_STRING_TYPE}

        if col[1] not in constants.FEATURE_STORE.RECOGNIZED_TF_RECORD_TYPES:
            raise InferTFRecordSchemaError("Could not recognize the spark type: {} for inferring the tf-records schema."
                                 "Recognized types are: {}".format(col[1],
                                                                   constants.FEATURE_STORE.RECOGNIZED_TF_RECORD_TYPES))
    return example, example_json


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
            constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE] == \
                constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_FIXED \
                and value[constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE] == constants.FEATURE_STORE.TF_RECORD_INT_TYPE:
            if constants.FEATURE_STORE.TF_RECORD_SCHEMA_SHAPE in value:
                example[str(key)] = tf.io.FixedLenFeature(shape=value[constants.FEATURE_STORE.TF_RECORD_SCHEMA_SHAPE],
                                                       dtype=tf.int64)
            else:
                example[str(key)] = tf.io.FixedLenFeature([], tf.int64)
        if value[
            constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE] == \
                constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_FIXED and \
                        value[constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE] == \
                        constants.FEATURE_STORE.TF_RECORD_FLOAT_TYPE:
            if constants.FEATURE_STORE.TF_RECORD_SCHEMA_SHAPE in value:
                example[str(key)] = tf.io.FixedLenFeature(shape=value[constants.FEATURE_STORE.TF_RECORD_SCHEMA_SHAPE],
                                                       dtype=tf.float32)
            else:
                example[str(key)] = tf.io.FixedLenFeature([], tf.float32)
        if value[
            constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE] == \
                constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_VAR and \
                        value[
                            constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE] == \
                        constants.FEATURE_STORE.TF_RECORD_INT_TYPE:
            example[str(key)] = tf.io.VarLenFeature(tf.int64)
        if value[
            constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE] == \
                constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_VAR and \
                        value[
                            constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE] == \
                        constants.FEATURE_STORE.TF_RECORD_FLOAT_TYPE:
            example[str(key)] = tf.io.VarLenFeature(tf.float32)
        if value[
            constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE] == \
                constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_VAR and \
                        value[
                            constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE] == \
                        constants.FEATURE_STORE.TF_RECORD_STRING_TYPE:
            example[str(key)] = tf.io.VarLenFeature(tf.string)
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
              hdfs_path + constants.DELIMITERS.SLASH_DELIMITER +
              constants.FEATURE_STORE.TRAINING_DATASET_TF_RECORD_SCHEMA_FILE_NAME)


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


def _do_get_latest_featuregroup_version(featuregroup_name, featurestore_metadata):
    """
    Utility method to get the latest version of a particular featuregroup

    Args:
        :featuregroup_name: the featuregroup to get the latest version of
        :featurestore_metadata: metadata of the featurestore

    Returns:
        the latest version of the featuregroup in the feature store
    """
    featuregroups = featurestore_metadata.featuregroups.values()
    matches = list(filter(lambda fg: fg.name == featuregroup_name, featuregroups))
    versions = list(map(lambda fg: int(fg.version), matches))
    if (len(versions) > 0):
        return max(versions)
    else:
        raise FeaturegroupNotFound("There was no version for featuregroup {} "
                               "found".format(featuregroup_name))


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
        :primary_key: list of names of the columns of the primary key

    Returns:
        True if the validation succeeded, otherwise raises an error

    Raises:
        :InvalidPrimaryKey: when one or more of the primary keys does not exist in the dataframe
    """
    cols = list(map(lambda x: x[0], featuregroup_df.dtypes))
    for pk in primary_key:
        if pk not in cols:
            raise InvalidPrimaryKey(
                "Invalid primary key: {}, the specified primary key does not exists among the available columns: {}" \
                    .format(primary_key, cols))
    return True



def _do_get_featuregroups(featurestore_metadata, online):
    """
    Gets a list of all featuregroups in a featurestore

    Args:
        :featurestore_metadata: the metadata of the featurestore
        :online: flag whether to filter the featuregroups that have online serving enabled

    Returns:
        A list of names of the featuregroups in this featurestore
    """
    featuregroups = featurestore_metadata.featuregroups.values()
    if online:
        featuregroups = list(filter(lambda fg: fg.is_online(), featuregroups))
    featuregroup_names = list(map(lambda fg: _get_table_name(fg.name, fg.version), featuregroups))
    return featuregroup_names


def _do_get_features_list(featurestore_metadata, online):
    """
    Gets a list of all features in a featurestore

    Args:
        :featurestore_metadata: metadata of the featurestore
        :online: flag whether to filter the featuregroups that have online serving enabled

    Returns:
        A list of names of the features in this featurestore
    """
    featuregroups = featurestore_metadata.featuregroups.values()
    if online:
        featuregroups = list(filter(lambda fg: fg.is_online(), featuregroups))
    features = []
    for fg in featuregroups:
        features.extend(fg.features)
    features = list(map(lambda f: f.name, features))
    return features


def _do_get_featuregroup_features_list(featuregroup, version, featurestore_metadata):
    """
    Gets a list of all names of features in a featuregroup in a featurestore.

    Args:
        :featuregroup: Featuregroup name.
        :version: Version of the featuregroup to use.
        :featurestore_metadata: Metadata of the featurestore.

    Returns:
        A list of names of the features in this featuregroup.
    """
    featuregroup_version = featuregroup + '_' + str(version)
    features = featurestore_metadata.featuregroups[featuregroup_version].features
    return list(map(lambda f: f.name, features))


def _do_get_training_dataset_features_list(training_dataset, version, featurestore_metadata):
    """
    Gets a list of all names of features in a training dataset in a featurestore.

    Args:
        :training_dataset: Training dataset name.
        :version: Version of the training dataset to use.
        :featurestore_metadata: Metadata of the featurestore.

    Returns:
        A list of names of the features in this training dataset.
    """
    training_dataset_version = training_dataset + '_' + str(version)
    features = featurestore_metadata.training_datasets[training_dataset_version].features
    return list(map(lambda f: f.name, features))


def _log(x):
    """
    Generic log function (in case logging is changed from stdout later)

    Args:
        :x: the argument to log

    Returns:
        None
    """
    print(x)


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
        map(lambda y: y[0], filter(lambda x: _is_type_numeric(x), spark_df.dtypes)))
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

    Raises:
        :ValueError: when the provided dataframe is of a structure that can't be used for computing certain statistics.
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


def _compute_cluster_analysis(spark_df, clusters=5):
    numeric_columns = list(map(lambda col_dtype: col_dtype[0], spark_df.dtypes))
    if (len(numeric_columns) == 0):
        raise ValueError("The provided spark dataframe does not contain any numeric columns. "
                         "Cannot compute cluster analysis with k-means on categorical columns. "
                         "The numeric datatypes are: {}" \
                         " and the number of numeric datatypes in the dataframe is: {} ({})".format(
            constants.SPARK_CONFIG.SPARK_NUMERIC_TYPES, len(spark_df.dtypes), spark_df.dtypes))
    if (len(numeric_columns) == 1):
        raise ValueError("The provided spark dataframe does contains only one numeric column. "
                         "Cluster analysis will filter out numeric columns and then "
                         "use pca to reduce dataset dimension to 2 dimensions and "
                         "then apply KMeans, this is not possible when the input data have only one numeric column."
                         "The numeric datatypes are: {}"
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

    Raises:
        :ValueError: when the provided dataframe is of a structure that can't be used for computing correlations.
    """
    numeric_columns = spark_df.dtypes
    if (len(numeric_columns) == 0):
        raise ValueError("The provided spark dataframe does not contain any numeric columns. "
                         "Cannot compute feature correlation on categorical columns. The numeric datatypes are: {}"
                         " and the number of numeric datatypes in the dataframe is: {} ({})".format(
            constants.SPARK_CONFIG.SPARK_NUMERIC_TYPES, len(spark_df.dtypes), spark_df.dtypes))
    if (len(numeric_columns) == 1):
        raise ValueError("The provided spark dataframe only contains one numeric column. "
                         "Cannot compute feature correlation on just one column. The numeric datatypes are: {}"
                         "and the number of numeric datatypes in the dataframe is: {} ({})".format(
            constants.SPARK_CONFIG.SPARK_NUMERIC_TYPES, len(spark_df.dtypes), spark_df.dtypes))
    if (len(numeric_columns) > constants.FEATURE_STORE.MAX_CORRELATION_MATRIX_COLUMNS):
        raise ValueError("The provided dataframe contains  {} columns, "
                         "feature correlation can only be computed for "
                         "dataframes with < {} columns due to scalability "
                         "reasons (number of correlatons grows "
                         "quadratically with the number of columns)" \
                         .format(len(numeric_columns), constants.FEATURE_STORE.MAX_CORRELATION_MATRIX_COLUMNS))
    spark_df_rdd = spark_df.rdd.map(lambda row: row[0:])
    corr_mat = Statistics.corr(spark_df_rdd, method=corr_method)
    pd_df_corr_mat = pd.DataFrame(corr_mat, columns=spark_df.columns, index=spark_df.columns)
    return pd_df_corr_mat


def _return_dataframe_type(dataframe, dataframe_type):
    """
    Helper method for returning the dataframe in spark/pandas/numpy/python, depending on user preferences

    Args:
        :dataframe: the spark dataframe to convert
        :dataframe_type: the type to convert to (spark,pandas,numpy,python)

    Returns:
        The dataframe converted to either spark, pandas, numpy or python.

    Raises:
        :CouldNotConvertDataframe: if the dataframe type is not supported
    """
    if dataframe_type == constants.FEATURE_STORE.DATAFRAME_TYPE_SPARK:
        return dataframe
    if dataframe_type == constants.FEATURE_STORE.DATAFRAME_TYPE_PANDAS:
        return dataframe.toPandas()
    if dataframe_type == constants.FEATURE_STORE.DATAFRAME_TYPE_NUMPY:
        return np.array(dataframe.collect())
    if dataframe_type == constants.FEATURE_STORE.DATAFRAME_TYPE_PYTHON:
        return dataframe.collect()

    raise CouldNotConvertDataframe("DataFrame type not supported")


def _convert_dataframe_to_spark(dataframe):
    """
    Helper method for converting a user-provided dataframe into a spark dataframe

    Args:
        :dataframe: the input dataframe (supported types are spark rdds, spark dataframes, pandas dataframes,
                    python 2D lists, and numpy 2D arrays)

    Returns:
        the dataframe convertd to a spark dataframe

    Raises:
        :CouldNotConvertDataframe: in case the provided dataframe could not be converted to a spark dataframe
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
            raise CouldNotConvertDataframe(
                "Cannot convert numpy array that do not have two dimensions to a dataframe. "
                "The number of dimensions are: {}".format(dataframe.ndim))
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
    raise CouldNotConvertDataframe(
        "The provided dataframe type is not recognized. Supported types are: spark rdds, spark dataframes, "
        "pandas dataframes, python 2D lists, and numpy 2D arrays. The provided dataframe has type: {}".format(
            type(dataframe)))


def _validate_metadata(name, schema, description, featurestore_settings):
    """
    Function for validating metadata when creating new feature groups and training datasets.
    Raises and assertion exception if there is some error in the metadata.

    Args:
        :name: the name of the feature group/training dataset
        :schema: the schema in the provided spark dataframe in terms of featureDTOs
        :description: the description
        :featurestore_regex: Regex string to match featuregroup/training dataset and feature names with

    Returns:
        None

    Raises:
        :ValueError: if the metadata does not match the required validation rules
    """
    if not featurestore_settings.featurestore_regex.match(name):
        raise ValueError("Illegal feature store entity name, the provided name {} is invalid. Entity names can only "
            "contain lower case characters, numbers and underscores and cannot be longer than {} characters or empty."
            .format(name, featurestore_settings.entity_name_max_len))

    if description:
        if len(description) > featurestore_settings.entity_description_max_len:
            raise ValueError("Illegal feature store entity description, the provided description for the entity {} is "
                "too long with {} characters. Entity descriptions cannot be longer than {} characters."
                .format(name, len(description), featurestore_settings.entity_description_max_len))

    if len(schema) == 0:
        raise ValueError("Cannot create a feature group from an empty spark dataframe")

    for f in schema:
        if not featurestore_settings.featurestore_regex.match(f[constants.REST_CONFIG.JSON_FEATURE_NAME]):
            raise ValueError("Illegal feature name, the provided feature name {} is invalid. Feature names can only "
                "contain lower case characters, numbers and underscores and cannot be longer than {} characters or "
                "empty.".format(f[constants.REST_CONFIG.JSON_FEATURE_NAME], featurestore_settings.entity_name_max_len))
        if (constants.REST_CONFIG.JSON_FEATURE_DESCRIPTION in f and 
            len(f[constants.REST_CONFIG.JSON_FEATURE_DESCRIPTION]) > featurestore_settings.entity_description_max_len):
            raise ValueError("Invalid feature description, the provided feature description of {} is too long with {} "
                "characters. Feature descriptions cannot be longer than {} characters."
                .format(f[constants.REST_CONFIG.JSON_FEATURE_NAME],
                    len(f[constants.REST_CONFIG.JSON_FEATURE_DESCRIPTION]),
                    featurestore_settings.entity_description_max_len))


def _convert_spark_dtype_to_hive_dtype(spark_dtype):
    """
    Helper function to convert a spark data type into a hive datatype

    Args:
        :spark_dtype: the spark datatype to convert

    Returns:
        the hive datatype or None

    Raises:
        :SparkToHiveSchemaConversionError: if there was an error converting a spark datatype to a hive datatype
    """
    if type(spark_dtype) is dict:
        if spark_dtype[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELD_TYPE].lower() == constants.SPARK_CONFIG.SPARK_ARRAY:
            return spark_dtype[
                       constants.SPARK_CONFIG.SPARK_SCHEMA_FIELD_TYPE].upper() + "<" + \
                   _convert_spark_dtype_to_hive_dtype(
                       spark_dtype[constants.SPARK_CONFIG.SPARK_SCHEMA_ELEMENT_TYPE]) + ">"
        if spark_dtype[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELD_TYPE].lower() == constants.SPARK_CONFIG.SPARK_STRUCT:
            struct_nested_fields = list(map(
                lambda field: field[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELD_NAME] +
                              constants.DELIMITERS.COLON_DELIMITER + _convert_spark_dtype_to_hive_dtype(
                    field[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELD_TYPE]),
                spark_dtype[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELDS]))
            return spark_dtype[
                       constants.SPARK_CONFIG.SPARK_SCHEMA_FIELD_TYPE].upper() + "<" + \
                   constants.DELIMITERS.COMMA_DELIMITER.join(struct_nested_fields) + ">"
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
        return spark_dtype.upper()
    raise SparkToHiveSchemaConversionError("Dataframe data type: {} not recognized.".format(spark_dtype))


def _convert_spark_dtype_to_mysql_dtype(spark_dtype):
    """
    Helper function to convert a spark data type into a mysql datatype

    Args:
        :spark_dtype: the spark datatype to convert

    Returns:
        the mysql datatype or None

    Raises:
        :SparkToMySQLSchemaConversionError: if there was an error converting a spark datatype to a mysql datatype
    """

    if spark_dtype.lower() == constants.SPARK_CONFIG.SPARK_LONG_TYPE:
        return constants.MYSQL_CONFIG.MYSQL_BIGINT_TYPE
    if spark_dtype.lower() == constants.SPARK_CONFIG.SPARK_SHORT_TYPE:
        return constants.MYSQL_CONFIG.MYSQL_BYTE_TYPE
    if spark_dtype.lower() == constants.SPARK_CONFIG.SPARK_BYTE_TYPE:
        return constants.MYSQL_CONFIG.MYSQL_CHAR_TYPE
    if spark_dtype.lower() == constants.SPARK_CONFIG.SPARK_INTEGER_TYPE \
            or spark_dtype.lower() == constants.SPARK_CONFIG.SPARK_INT_TYPE:
        return constants.MYSQL_CONFIG.MYSQL_INTEGER_TYPE
    if spark_dtype.lower() == constants.SPARK_CONFIG.SPARK_BIGINT_TYPE:
        return constants.MYSQL_CONFIG.MYSQL_BIGINT_TYPE
    if spark_dtype.lower() == constants.SPARK_CONFIG.SPARK_SMALLINT_TYPE:
        return constants.MYSQL_CONFIG.MYSQL_SMALLINT_TYPE
    if spark_dtype.lower() == constants.SPARK_CONFIG.SPARK_STRING_TYPE:
        return constants.MYSQL_CONFIG.MYSQL_VARCHAR_1000_TYPE
    if spark_dtype.lower() == constants.SPARK_CONFIG.SPARK_BINARY_TYPE:
        return constants.MYSQL_CONFIG.MYSQL_BLOB_TYPE
    if constants.SPARK_CONFIG.SPARK_STRUCT in spark_dtype.lower():
        return constants.MYSQL_CONFIG.MYSQL_BLOB_TYPE
    if constants.SPARK_CONFIG.SPARK_ARRAY in spark_dtype.lower():
        return constants.MYSQL_CONFIG.MYSQL_BLOB_TYPE
    if constants.SPARK_CONFIG.SPARK_VECTOR in spark_dtype.lower():
        return constants.MYSQL_CONFIG.MYSQL_BLOB_TYPE
    if spark_dtype.lower() in constants.MYSQL_CONFIG.MYSQL_DATA_TYPES:
        return spark_dtype.lower()
    if spark_dtype.upper() in constants.MYSQL_CONFIG.MYSQL_DATA_TYPES:
        return spark_dtype.upper()
    raise SparkToMySQLSchemaConversionError("Dataframe data type: {} not recognized.".format(spark_dtype))


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


def _do_prepare_stats_settings(featuregroup, featuregroup_version, featurestore_metadata, descriptive_statistics,
                               feature_correlation, feature_histograms, cluster_analysis, stat_columns, num_bins,
                               num_clusters, corr_method):
    """
    Sanitizes the input and replaces the None settings with the current setting instead.

    Args:
        :featuregroup: the featuregroup concerned
        :featuregroup_version: the version of the featuregroup
        :featurestore_metadata: the featurestore metadata to compare against
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
        [type]: [description]
    """
    table_name = _get_table_name(featuregroup, featuregroup_version)
    fg = featurestore_metadata.featuregroups[table_name]
    result = {
        "desc_stats_enabled": descriptive_statistics if descriptive_statistics else fg.desc_stats_enabled,
        "feat_corr_enabled": feature_correlation if feature_correlation else fg.feat_corr_enabled,
        "feat_hist_enabled": feature_histograms if feature_histograms else fg.feat_hist_enabled,
        "cluster_analysis_enabled": cluster_analysis if cluster_analysis else fg.cluster_analysis_enabled,
        "stat_columns": stat_columns if stat_columns else fg.stat_columns,
        "num_bins": num_bins if num_bins else fg.num_bins,
        "num_clusters": num_clusters if num_clusters else fg.num_clusters,
        "corr_method": corr_method if corr_method else fg.corr_method
    }
    return result


def _do_get_project_featurestore():
    """
    Gets the project's featurestore name (project_featurestore)

    Returns:
        the project's featurestore name

    """
    project_name = hdfs.project_name()
    featurestore_name = project_name.lower() + constants.FEATURE_STORE.FEATURESTORE_SUFFIX
    return featurestore_name


def _do_get_project_training_datasets_sink():
    """
    Gets the project's default location for storing training datasets in HopsFS

    Returns:
        the project's default hopsfs location for storing training datasets

    """
    project_name = hdfs.project_name()
    training_datasets_sink = project_name + constants.FEATURE_STORE.TRAINING_DATASETS_SUFFIX
    return training_datasets_sink


def _visualization_validation_warning():
    """
    Checks whether the user is trying to do visualization inside a livy session and prints a warning message
    if the user is trying to plot inside the livy session.

    Returns:
        None

    """
    if constants.ENV_VARIABLES.LIVY_VERSION_ENV_VAR in os.environ:
        _log("Visualizations are not supported in Livy Sessions. "
                      "Use %%local and %matplotlib inline to access the "
                      "python kernel from PySpark notebooks")


def _matplotlib_magic_reminder():
    """
    Prints a reminder message to the user to enable matplotlib inline when plotting inside Jupyter notebooks

    Returns:
        None

    """
    _log("Remember to add %%matplotlib inline when doing visualizations in Jupyter notebooks")


def _is_hive_enabled(spark):
    """
    Checks whether Hive is enabled for a given spark session\

    Args:
         :spark: the spark session to verify

    Returns:
        true if hive is enabled, otherwise false
    """
    return _get_spark_sql_catalog_impl(spark) == constants.SPARK_CONFIG.SPARK_SQL_CATALOG_HIVE


def _get_spark_sql_catalog_impl(spark):
    """
    Gets the sparkSQL catalog implementatin of a given spark session

    Args:
         :spark: the spark session to get the SQL catalog implementation of

    Returns:
        the sparkSQL catalog implementation of the spark session
    """
    return dict(spark.sparkContext._conf.getAll())[constants.SPARK_CONFIG.SPARK_SQL_CATALOG_IMPLEMENTATION]


def _get_training_dataset_type_info(featurestore_metadata, external=False):
    """
    Gets the type information of a training dataset that the backend expects

    Args:
         :featurestore_metadata: metadata of the featurestore
         :external: whether it is an external featuregroup or not

    Returns:
        the type information of the training dataset
    """
    if external:
        training_dataset_type = featurestore_metadata.settings.external_training_dataset_type
    else:
        training_dataset_type = featurestore_metadata.settings.hopsfs_training_dataset_type
    return training_dataset_type


def _get_external_training_dataset_path(path):
    """
    The location Hopsworks returns cointain the filesystem which is set to s3://. Spark expects the filesystem to be s3a:// 

    Args:
        path: the path returned by Hopsworks

    Returns:
        the s3 path to read the training dataset from Spark 
    """

    return path.replace("s3", "s3a", 1)


def _setup_s3_credentials_for_spark(access_key, secret_key, spark):
    """
    Registers the access key and secret key environment variables for writing to S3 with Spark

    Args:
        :access_key: the S3 access key ID
        :secret_key: the S3 secret key
        :spark: the spark session

    Returns:
        None
    """
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set(constants.S3_CONFIG.S3_ACCESS_KEY_ENV, access_key)
    sc._jsc.hadoopConfiguration().set(constants.S3_CONFIG.S3_SECRET_KEY_ENV, secret_key)


def _get_bucket_path(bucket, dataset_path):
    """
    Utility function for getting the S3 path of a feature dataset on S3

    Args:
        :bucket: the bucket of the storage connector for s3
        :dataset_path: the user-supplied path to the dataset

    Returns:
        S3 path to the dataset (bucket and file prefix appended if not in the user-supplied path

    """
    if bucket in dataset_path:
        if constants.S3_CONFIG.S3_FILE_PREFIX not in dataset_path:
            return constants.S3_CONFIG.S3_FILE_PREFIX + dataset_path
        return dataset_path
    else:
        path = ""
        if constants.S3_CONFIG.S3_FILE_PREFIX not in bucket:
            path = constants.S3_CONFIG.S3_FILE_PREFIX
        path = path + bucket + constants.DELIMITERS.SLASH_DELIMITER + dataset_path
        return path


def _add_provenance_metadata_to_dataframe(spark_df, feature_to_featuregroup_mapping):
    """
    Adds metadata of which featuregroup a certain feature was fetched from to the spark dataframe metadata.
    This metadata is used by hopsworks provenance framework to track which features were used to create training
    datasets

    Args:
        :spark_df: the spark dataframe to add the metadata to
        :feature_to_featuregroup_mapping: a mapping of feature to featuregroup

    Returns:
        the updated spark dataframe with the added metadata
    """
    select = []
    for f, fg in feature_to_featuregroup_mapping.items():
        meta = _get_column_metadata(json.loads(spark_df.schema.json()), f)
        parts = fg.rsplit('_',1)
        fg_name = parts[0]
        fg_version = parts[1]
        meta[constants.FEATURE_STORE.TRAINING_DATASET_PROVENANCE_FEATUREGROUP] = fg_name
        meta[constants.FEATURE_STORE.TRAINING_DATASET_PROVENANCE_VERSION] = fg_version
        select.append(col(f).alias(f, metadata=meta))

    return spark_df.select(select)


def _get_column_metadata(spark_schema, column_name):
    """
    Gets the metadata of a given column from the spark schema

    Args:
        :spark_schema: the spark schema
        :column_name: the name of the column to get the metadata of
        :featuregroup_name: name of the featuregroup

    Returns:
        the metadata dict
    """
    for field in spark_schema[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELDS]:
        if field[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELD_NAME] == column_name:
            return field[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELD_METADATA]
    raise FeatureNotFound("feature {} was not found".format(column_name))