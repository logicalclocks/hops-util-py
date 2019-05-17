from hops import constants
from hops.featurestore_impl.dao.featurestore_entity import FeaturestoreEntity

class Featuregroup(FeaturestoreEntity):
    """
    Represents an individual featuregroup in the featurestore (Hive Table)
    """

    def __init__(self, featuregroup_json):
        """
        Initialize the feature group from JSON payload

        Args:
            :featuregroup_json: JSON representation of the featuregroup, returned from Hopsworks REST API
        """
        self.description = featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_DESCRIPTION]
        self.features = self._parse_features(featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES])
        self.created = featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_CREATED]
        self.creator = featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_CREATOR]
        self.dependencies = \
            self._parse_dependencies(featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_DEPENDENCIES])
        self.job_name = featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_JOBNAME]
        self.name = featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_NAME]
        self.version = featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION]
        self.hdfs_path = featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_HDFS_PATH]
        self.last_computed = featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_LAST_COMPUTED]
        self.inode_id = featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_INODE_ID]
        self.id = featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_ID]


    def __lt__(self, other):
        return self.name.__lt__(other.name)