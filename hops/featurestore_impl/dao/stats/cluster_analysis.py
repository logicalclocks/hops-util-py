from hops.featurestore_impl.dao.stats.datapoint import DataPoint
from hops import constants
from hops.featurestore_impl.dao.stats.cluster import Cluster


class ClusterAnalysis(object):
    """
    Represents Cluster Analysis computed for a featuregroup or training dataset in the featurestore
    """

    def __init__(self, cluster_analysis_json):
        """
        Initialize the cluster analysis object from JSON payload

        Args:
            :cluster_analysis_json: JSON data of the cluster analysis
        """
        self.datapoints = self._parse_data_points(cluster_analysis_json)
        self.clusters = self._parse_clusters(cluster_analysis_json)


    def _parse_data_points(self, cluster_analysis_json):
        """
        Parse list of datapoints in the cluster analysis from the JSON payload

        Args:
            :cluster_analysis_json: the JSON payload with the datapoints

        Returns:
            A list of the parsed datapoints
        """
        datapoints = []
        for datapoint_json in cluster_analysis_json[constants.REST_CONFIG.JSON_CLUSTERING_ANALYSIS_DATA_POINTS]:
            datapoint = DataPoint(datapoint_json)
            datapoints.append(datapoint)
        return datapoints


    def _parse_clusters(self, cluster_analysis_json):
        """
        Parse list of clusters in the cluster analysis from the JSON payload

        Args:
            :cluster_analysis_json: the JSON payload with the datapoints

        Returns:
            A list of the parsed clusters
        """
        clusters = []
        for cluster_json in cluster_analysis_json[constants.FEATURE_STORE.CLUSTERING_ANALYSIS_CLUSTERS_OUTPUT_COLUMN]:
            cluster = Cluster(cluster_json)
            clusters.append(cluster)
        return clusters