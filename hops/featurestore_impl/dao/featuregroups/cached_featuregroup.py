from hops import constants

class CachedFeaturegroup():
    """
    Represents a cached featuregroup in the featurestore
    """

    def __init__(self, cached_featuregroup_json):
        """
        Initialize the cached feature group from JSON payload

        Args:
            :cached_featuregroup_json: JSON representation of the featuregroup, returned from Hopsworks REST API
        """
        self.hive_table_id = cached_featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_HIVE_TBL_ID]
        self.hdfs_store_paths = cached_featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_HDFS_STORE_PATHS]
        self.input_format = cached_featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_INPUT_FORMAT]
        self.hive_table_type = cached_featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_HIVE_TABLE_TYPE]
        self.inode_id = cached_featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_INODE_ID]