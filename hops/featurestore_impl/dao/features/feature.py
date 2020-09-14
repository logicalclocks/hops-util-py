from hops import constants

class Feature(object):
    """
    Represents an individual feature in the feature store in a feature group
    """

    def __init__(self, feature_json):
        """
        Initialize the feature from JSON payload

        Args:
            :feature_json: JSON data about the feature returned from Hopsworks REST API
        """
        self.name = feature_json[constants.REST_CONFIG.JSON_FEATURE_NAME]
        self.type = feature_json[constants.REST_CONFIG.JSON_FEATURE_TYPE]
        if constants.REST_CONFIG.JSON_FEATURE_DESCRIPTION in feature_json:
            self.description = feature_json[constants.REST_CONFIG.JSON_FEATURE_DESCRIPTION]
        else:
            self.description = ""
        self.primary = feature_json[constants.REST_CONFIG.JSON_FEATURE_PRIMARY]
        self.partition = feature_json[constants.REST_CONFIG.JSON_FEATURE_PARTITION]
        if constants.REST_CONFIG.JSON_FEATURE_ONLINE_TYPE in feature_json:
            self.online_type = feature_json[constants.REST_CONFIG.JSON_FEATURE_ONLINE_TYPE]
        else:
            self.online_type = ""