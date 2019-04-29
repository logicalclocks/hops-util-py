from hops import constants

class FeaturestoreDependency(object):
    """
    Represents a data dependency of a feature group or a training dataset in the feature store
    """

    def __init__(self, dependency_json):
        """
        Initialize the featurestore dependency from JSON payload

        Args:
            :dependency_json: JSON data about the dependency returned from Hopsworks REST API
        """
        self.path = dependency_json[constants.REST_CONFIG.JSON_FEATURESTORE_DEPENDENCY_PATH]
        self.modification = dependency_json[constants.REST_CONFIG.JSON_FEATURESTORE_DEPENDENCY_MODIFICATION_DATE]
        self.inode_id = dependency_json[constants.REST_CONFIG.JSON_FEATURESTORE_DEPENDENCY_INODE_ID]
        self.dir = dependency_json[constants.REST_CONFIG.JSON_FEATURESTORE_DEPENDENCY_DIR]