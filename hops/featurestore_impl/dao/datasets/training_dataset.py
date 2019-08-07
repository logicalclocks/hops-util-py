from hops import constants
from hops.featurestore_impl.dao.common.featurestore_entity import FeaturestoreEntity
from hops.featurestore_impl.dao.datasets.external_training_dataset import ExternalTrainingDataset
from hops.featurestore_impl.dao.datasets.hopsfs_training_dataset import HopsfsTrainingDataset


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
        self.features = \
            self._parse_features(training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_FEATURES])
        self.id = training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_ID]
        self.name = training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_NAME]
        self.version = training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_VERSION]
        self.data_format = training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_FORMAT]
        self.training_dataset_type = training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_TYPE]
        if(self.training_dataset_type == constants.REST_CONFIG.JSON_TRAINING_DATASET_HOPSFS_TYPE):
            self.hopsfs_training_dataset = HopsfsTrainingDataset(training_dataset_json)
        if(self.training_dataset_type == constants.REST_CONFIG.JSON_TRAINING_DATASET_EXTERNAL_TYPE):
            self.external_training_dataset = ExternalTrainingDataset(training_dataset_json)

