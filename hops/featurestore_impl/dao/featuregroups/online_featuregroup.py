from hops import constants

class OnlineFeaturegroup():
    """
    Represents an online featuregroup in the featurestore
    """

    def __init__(self, online_featuregroup_json):
        """
        Initialize the online feature group from JSON payload

        Args:
            :online_featuregroup_json: JSON representation of the online featuregroup, returned from Hopsworks REST API
        """
        self.online_featuregroup_id = online_featuregroup_json[constants.REST_CONFIG.JSON_ONLINE_FEATUREGROUP_ID]
        self.db = online_featuregroup_json[constants.REST_CONFIG.JSON_ONLINE_FEATUREGROUP_DB]
        self.table = online_featuregroup_json[constants.REST_CONFIG.JSON_ONLINE_FEATUREGROUP_TABLE]
        if constants.REST_CONFIG.JSON_ONLINE_FEATUREGROUP_SIZE in online_featuregroup_json:
            self.size = online_featuregroup_json[constants.REST_CONFIG.JSON_ONLINE_FEATUREGROUP_SIZE]
        else:
            self.size = 0
        if constants.REST_CONFIG.JSON_ONLINE_FEATUREGROUP_TABLE_ROWS in online_featuregroup_json:
            self.table_rows = online_featuregroup_json[constants.REST_CONFIG.JSON_ONLINE_FEATUREGROUP_TABLE_ROWS]
        else:
            self.table_rows = 0
        if constants.REST_CONFIG.JSON_ONLINE_FEATUREGROUP_TABLE_ROWS in online_featuregroup_json:
            self.table_type = online_featuregroup_json[constants.REST_CONFIG.JSON_ONLINE_FEATUREGROUP_TABLE_TYPE]
        else:
            self.table_type = "BASE_TABLE"