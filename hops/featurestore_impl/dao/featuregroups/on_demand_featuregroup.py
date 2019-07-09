from hops import constants

class OnDemandFeaturegroup():
    """
    Represents an on-demand featuregroup in the featurestore
    """

    def __init__(self, on_demand_featuregroup_json):
        """
        Initialize the on-demand feature group from JSON payload

        Args:
            :on_demand_featuregroup_json: JSON representation of the featuregroup, returned from Hopsworks REST API
        """
        self.jdbc_connector_id = on_demand_featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_JDBC_CONNECTOR_ID]
        self.jdbc_connector_name = on_demand_featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_JDBC_CONNECTOR_NAME]
        self.query = on_demand_featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_ON_DEMAND_QUERY]