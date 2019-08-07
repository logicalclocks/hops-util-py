from hops import constants
from hops.featurestore_impl.dao.stats.descriptive_stats_metric_value import DescriptiveStatsMetricValue


class DescriptiveStatsMetricValues(object):
    """
    Represents a descriptive stats metric values for a feature in a feature group or training dataset
    """

    def __init__(self, metric_values_json):
        """
        Initialize the metric values object from JSON payload

        Args:
            :metric_values_json: JSON data of the metric values
        """
        self.metric_values = self._parse_descriptive_stats_metric_values(metric_values_json)
        self.feature_name = metric_values_json[constants.REST_CONFIG.JSON_DESCRIPTIVE_STATS_FEATURE_NAME]


    def _parse_descriptive_stats_metric_values(self, metric_values_json):
        """
        Parse list of metric values in the descriptive stats JSON payload

        Args:
            :metric_values_json: the JSON payload with the metric values

        Returns:
            A list of the parsed metric values
        """
        metric_values = []
        for metric_value_json in metric_values_json[constants.REST_CONFIG.JSON_DESCRIPTIVE_STATS_METRIC_VALUES]:
            metric_value = DescriptiveStatsMetricValue(metric_value_json)
            metric_values.append(metric_value)
        return metric_values
