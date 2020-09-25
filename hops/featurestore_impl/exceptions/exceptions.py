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

class HiveNotEnabled(Exception):
    """
    This exception will be raised if the user tries to peform featurestore operations using SparkSQL when
    hive is not enabled.
    """

class StorageConnectorNotFound(Exception):
    """This exception will be raised if a requested storage connector cannot be found"""


class CannotInsertIntoOnDemandFeatureGroup(Exception):
    """
    This exception will be raised if the user calls featurestore.insert_into_featuregroup(fg1)
    where fg1 is an on-demand feature group
    """

class CannotUpdateStatisticsOfOnDemandFeatureGroup(Exception):
    """
    This exception will be raised if the user calls featurestore.update_featuregroup_stats(fg1)
    where fg1 is an on-demand feature group
    """

class CannotGetPartitionsOfOnDemandFeatureGroup(Exception):
    """
    This exception will be raised if the user calls featurestore.get_featuregroup_partitions(fg1)
    where fg1 is an on-demand feature group
    """

class NumpyDatasetFormatNotSupportedForExternalTrainingDatasets(Exception):
    """
    This exception will be raised if the user tries to create an external training dataset with the .npy format.
    .npy datasets are not supported for external training datasets.
    """

class HDF5DatasetFormatNotSupportedForExternalTrainingDatasets(Exception):
    """
    This exception will be raised if the user tries to create an external training dataset with the .npy format.
    .npy datasets are not supported for external training datasets.
    """

class StorageConnectorTypeNotSupportedForFeatureImport(Exception):
    """
    This exception will be raised if the user tries to import a feature group using a storage connector type
    that is not supported
    """

class OnlineFeaturestorePasswordOrUserNotFound(Exception):
    """
    This exception will be raised if the user tries to do an operation on the online feature store but a
    user/password for the online featurestore was not found.
    """

class SparkToMySQLSchemaConversionError(Exception):
    """This exception will be raised if there is an error in translating the spark schema to a MySQL schema"""


class CannotEnableOnlineFeatureServingForOnDemandFeatureGroup(Exception):
    """
    This exception will be raised if the user calls featurestore.enable_featuregroup_online(fg1)
    where fg1 is an on-demand feature group
    """


class CannotDisableOnlineFeatureServingForOnDemandFeatureGroup(Exception):
    """
    This exception will be raised if the user calls featurestore.disable_featuregroup_online(fg1)
    where fg1 is an on-demand feature group
    """

class OnlineFeaturestoreNotEnabled(Exception):
    """
    This exception will be raised if the user tries to interact with the online featurestore but it is not enabled
    """