from hops import constants
from hops.featurestore_impl.dao.featurestore_entity import FeaturestoreEntity

class TrainingDataset(FeaturestoreEntity):
    """
    Represents a training dataset in the feature store
    """

    def __init__(self, training_dataset_json):
        """
        Initalizes the training dataset from JSON payload

        Args:
            :training_dataset_json: JSON data about the training dataset returned from Hopsworks REST API
        """
        self.creator = training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_CREATOR]
        self.created = training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_CREATED]
        self.description = training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_DESCRIPTION]
        self.dependencies = \
            self._parse_dependencies(training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_DEPENDENCIES])
        self.features = \
            self._parse_features(training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_FEATURES])
        self.hdfs_path = training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_HDFS_STORE_PATH]
        self.last_computed = training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_LAST_COMPUTED]
        self.inode_id = training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_INODE_ID]
        self.id = training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_ID]
        self.job_name = training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_JOBNAME]
        self.name = training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_NAME]
        self.version = training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_VERSION]
        self.data_format = training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_FORMAT]

