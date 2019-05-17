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

class RestAPIError(Exception):
    """This exception will be raised if there is an error response from a REST API call to Hopsworks"""
