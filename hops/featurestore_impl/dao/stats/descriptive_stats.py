from hops import constants
from hops.featurestore_impl.dao.stats.descriptive_stats_metric_values import DescriptiveStatsMetricValues


class DescriptiveStats(object):
    """
    Represents descriptive statistics computed for a featuregroup or training dataset in the featurestore
    """

    def __init__(self, descriptive_stats_json):
        """
        Initialize the descriptive statistics object from JSON payload

        Args:
            :descriptive_stats_json: JSON data of the descriptive statistics
        """
        self.descriptive_stats = self._parse_descriptive_stats_metrics(descriptive_stats_json)


    def _parse_descriptive_stats_metrics(self, descriptive_stats_json):
        """
        Parse list of descriptive stats for features JSON payload

        Args:
            :descriptive_stats_json: the JSON payload with the descriptive statistics for the list of features

        Returns:
            A list of the parsed descriptive statistics
        """
        descriptive_stats = []
        for metric_values_json in descriptive_stats_json[constants.REST_CONFIG.JSON_DESCRIPTIVE_STATS]:
            metric_values = DescriptiveStatsMetricValues(metric_values_json)
            descriptive_stats.append(metric_values)
        return descriptive_stats