from hops import constants

class DescriptiveStatsMetricValue(object):
    """
    Represents a Metric Value in Descriptive Stats computed for a featuregroup or training dataset in the featurestore
    """

    def __init__(self, metric_value_json):
        """
        Initialize the metric value object from JSON payload

        Args:
            :metric_value_json: JSON data of the metric value
        """
        self.metric_name = metric_value_json[constants.FEATURE_STORE.DESCRIPTIVE_STATS_METRIC_NAME_COL]
        self.value = float(metric_value_json[constants.FEATURE_STORE.DESCRIPTIVE_STATS_VALUE_COL])