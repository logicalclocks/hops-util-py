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