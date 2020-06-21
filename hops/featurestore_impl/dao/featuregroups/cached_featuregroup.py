from hops import constants
from hops.featurestore_impl.dao.featuregroups.online_featuregroup import OnlineFeaturegroup

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
        self.online_enabled = cached_featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_ONLINE]
        self.hudi_enabled = cached_featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_HUDI]