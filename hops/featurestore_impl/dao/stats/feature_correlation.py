from hops import constants
from hops.featurestore_impl.dao.stats.correlation_value import CorrelationValue


class FeatureCorrelation(object):
    """
    Represents a Feature Correlation in a Correlation Matrix computed for a featuregroup or training dataset
    in the featurestore
    """

    def __init__(self, feature_correlation_json):
        """
        Initialize the cluster object from JSON payload

        Args:
            :feature_correlation_json: JSON data of the feature correlation
        """
        self.feature_name = feature_correlation_json[constants.REST_CONFIG.JSON_CORRELATION_FEATURE_NAME]
        self.correlation_values = self._parse_feature_correlation(feature_correlation_json)


    def _parse_feature_correlation(self, feature_correlation_json):
        """
        Parse list of feature correlations in the JSON payload

        Args:
            :feature_correlation_json: the JSON payload with the feature correlations

        Returns:
            A list of the feature correlations
        """
        feature_correlation = []
        for correlation_value_json in feature_correlation_json[constants.REST_CONFIG.JSON_CORRELATION_VALUES]:
            correlation_value = CorrelationValue(correlation_value_json)
            feature_correlation.append(correlation_value)
        return feature_correlation