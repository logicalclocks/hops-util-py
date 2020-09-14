from hops import constants
from hops.featurestore_impl.dao.common.featurestore_entity import FeaturestoreEntity
from hops.featurestore_impl.dao.features.training_dataset_feature import TrainingDatasetFeature


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
        self.location = training_dataset_json[constants.REST_CONFIG.JSON_FEATURESTORE_LOCATION]
        self.connector_id = training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_CONNECTOR_ID]
        self.connector_name = training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_CONNECTOR_NAME]


    def _parse_features(self, features_json):
        """
        Parses a list of features in JSON format into a list of Feature objects

        Args:
            :features_json: json representation of the list of features

        Returns:
            a list of Feature objects
        """
        return list(map(lambda feature_json: TrainingDatasetFeature(feature_json), features_json))

