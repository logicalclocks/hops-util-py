from hops import constants
from hops.featurestore_impl.dao.stats.feature_correlation import FeatureCorrelation


class CorrelationMatrix(object):
    """
    Represents correlation matrix computed for a featuregroup or training dataset in the featurestore
    """

    def __init__(self, correlation_matrix_json):
        """
        Initialize the correlation matrix object from JSON payload

        Args:
            :correlation_matrix_json: JSON data of the correlation_matrix
        """
        self.feature_correlations = self._parse_feature_correlations(correlation_matrix_json)


    def _parse_feature_correlations(self, correlation_matrix_json):
        """
        Parse list of feature correlations in correlation matrix from the JSON payload

        Args:
            :correlation_matrix_json: the JSON payload with the feature correlations

        Returns:
            A list of the parsed feature correlations
        """
        feature_correlations = []
        for feature_correlation_json in correlation_matrix_json[constants.REST_CONFIG.JSON_FEATURE_CORRELATIONS]:
            feature_correlation = FeatureCorrelation(feature_correlation_json)
            feature_correlations.append(feature_correlation)
        return feature_correlations