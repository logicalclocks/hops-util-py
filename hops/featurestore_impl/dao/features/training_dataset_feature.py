from hops import constants

class TrainingDatasetFeature(object):
    """
    Represents a feature in a training dataset
    """

    def __init__(self, feature_json):
        """
        Initialize the feature from JSON payload

        Args:
            :feature_json: JSON data about the feature returned from Hopsworks REST API
        """
        self.name = feature_json[constants.REST_CONFIG.JSON_FEATURE_NAME]
        self.type = feature_json[constants.REST_CONFIG.JSON_FEATURE_TYPE]
        self.index = feature_json[constants.REST_CONFIG.JSON_FEATURE_INDEX]