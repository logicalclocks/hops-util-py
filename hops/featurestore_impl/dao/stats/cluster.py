from hops import constants

class Cluster(object):
    """
    Represents a Cluster in Cluster Analysis computed for a featuregroup or training dataset in the featurestore
    """

    def __init__(self, cluster_json):
        """
        Initialize the cluster object from JSON payload

        Args:
            :cluster_json: JSON data of the cluster
        """
        self.datapoint_name = cluster_json[constants.REST_CONFIG.JSON_CLUSTERING_ANALYSIS_DATA_POINT_NAME]
        self.cluster = int(cluster_json[constants.REST_CONFIG.JSON_CLUSTERING_ANALYSIS_CLUSTER])