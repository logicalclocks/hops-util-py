from hops import constants

class Featurestore(object):
    """
    Represents a feature store in Hopsworks
    """

    def __init__(self, featurestore_json):
        """
        Initialize the feature store from JSON payload

        Args:
            :featurestore_json: JSON data about the featurestore returned from Hopsworks REST API
        """
        self.id = featurestore_json[constants.REST_CONFIG.JSON_FEATURESTORE_ID]
        self.name = featurestore_json[constants.REST_CONFIG.JSON_FEATURESTORE_NAME]
        self.project_id = featurestore_json[constants.REST_CONFIG.JSON_FEATURESTORE_PROJECT_ID]
        self.description = featurestore_json[constants.REST_CONFIG.JSON_FEATURESTORE_DESCRIPTION]
        self.hdfs_path = featurestore_json[constants.REST_CONFIG.JSON_FEATURESTORE_HDFS_PATH]
        self.project_name = featurestore_json[constants.REST_CONFIG.JSON_FEATURESTORE_PROJECT_NAME]
        self.inode_id = featurestore_json[constants.REST_CONFIG.JSON_FEATURESTORE_INODE_ID]
        if constants.REST_CONFIG.JSON_FEATURESTORE_ONLINE_ENABLED in featurestore_json:
            self.online_enabled = featurestore_json[constants.REST_CONFIG.JSON_FEATURESTORE_ONLINE_ENABLED]
        else:
            self.online_enabled = False
        if constants.REST_CONFIG.JSON_FEATURESTORE_ONLINE_FEATURESTORE_TYPE in featurestore_json:
            self.online_featurestore_type = \
                featurestore_json[constants.REST_CONFIG.JSON_FEATURESTORE_ONLINE_FEATURESTORE_TYPE]
        else:
            self.online_featurestore_type = None
        if constants.REST_CONFIG.JSON_FEATURESTORE_OFFLINE_FEATURESTORE_TYPE in featurestore_json:
            self.offline_featurestore_type = \
                featurestore_json[constants.REST_CONFIG.JSON_FEATURESTORE_OFFLINE_FEATURESTORE_TYPE]
        else:
            self.offline_featurestore_type = None
        if constants.REST_CONFIG.JSON_FEATURESTORE_ONLINE_FEATURESTORE_NAME in featurestore_json:
            self.online_featurestore_name = \
                featurestore_json[constants.REST_CONFIG.JSON_FEATURESTORE_ONLINE_FEATURESTORE_NAME]
        else:
            self.online_featurestore_name = None
        if constants.REST_CONFIG.JSON_FEATURESTORE_OFFLINE_FEATURESTORE_NAME in featurestore_json:
            self.offline_featurestore_name = \
                featurestore_json[constants.REST_CONFIG.JSON_FEATURESTORE_OFFLINE_FEATURESTORE_NAME]
        else:
            self.offline_featurestore_name = None
