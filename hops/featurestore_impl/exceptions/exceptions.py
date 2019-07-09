"""
Exceptions thrown by the feature store python client
"""

class FeaturegroupNotFound(Exception):
    """This exception will be raised if a requested featuregroup cannot be found"""

class FeatureNotFound(Exception):
    """This exception will be raised if a requested feature cannot be found"""

class FeatureNameCollisionError(Exception):
    """This exception will be raised if a requested feature cannot be uniquely identified in the feature store"""

class InferJoinKeyError(Exception):
    """This exception will be raised if a join key for a featurestore query cannot be inferred"""

class InvalidPrimaryKey(Exception):
    """This exception will be raised if a user specified an invalid primary key of a featuregroup"""

class TrainingDatasetNotFound(Exception):
    """This exception will be raised if a requested training dataset cannot be found"""

class CouldNotConvertDataframe(Exception):
    """This exception will be raised if a dataframe cannot be converted to/from spark"""

class InferTFRecordSchemaError(Exception):
    """This exception will be raised if there is an error in inferring the tfrecord schema of a dataframe"""

class TFRecordSchemaNotFound(Exception):
    """This exception will be raised if the tfrecord schema for a training dataset could not be found"""

class HiveDatabaseNotFound(Exception):
    """This exception will be raised if the hive database of the featurestore could not be found"""

class SparkToHiveSchemaConversionError(Exception):
    """This exception will be raised if there is an error in translating the spark schema to a hive schema"""

class FeatureVisualizationError(Exception):
    """This exception will be raised if there is an error in visualization feature statistics"""

class FeatureDistributionsNotComputed(Exception):
    """
    This exception will be raised if the user tries to visualize feature distributions
    for a feature group or training dataset for which the distributions have not been computed
    """

class FeatureCorrelationsNotComputed(Exception):
    """
    This exception will be raised if the user tries to visualize feature correlations
    for a feature group or training dataset for which the correlations have not been computed
    """

class FeatureClustersNotComputed(Exception):
    """
    This exception will be raised if the user tries to visualize feature clusters
    for a feature group or training dataset for which the clusters have not been computed
    """

class DescriptiveStatisticsNotComputed(Exception):
    """
    This exception will be raised if the user tries to visualize feature clusters
    for a feature group or training dataset for which the clusters have not been computed
    """


class HiveNotEnabled(Exception):
    """
    This exception will be raised if the user tries to peform featurestore operations using SparkSQL when
    hive is not enabled.
    """

class StatisticsComputationError(Exception):
    """
    This exception will be raised if there is an error computing the statistics of a feature group or
    training dataset
    """

class StorageConnectorNotFound(Exception):
    """This exception will be raised if a requested storage connector cannot be found"""