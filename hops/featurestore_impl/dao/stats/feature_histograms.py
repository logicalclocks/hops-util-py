from hops.featurestore_impl.dao.stats.feature_histogram import FeatureHistogram


class FeatureHistograms(object):
    """
    Represents feature histogram for a feature in a featuregroup or training dataset
    """

    def __init__(self, feature_histograms_json):
        """
        Initialize the histogram object from JSON payload

        Args:
            :feature_histograms_json: JSON data of the feature histogram
        """
        self.feature_distributions = self._parse_feature_distributions(feature_histograms_json)


    def _parse_feature_distributions(self, feature_histograms_json):
        """
        Parse list of feature histograms in the JSON payload

        Args:
            :feature_histogram_json: the JSON payload with the feature histograms

        Returns:
            A list of the parsed feature histograms
        """
        feature_histograms = []
        for feature_histogram_json in feature_histograms_json:
            feature_histogram = FeatureHistogram(feature_histogram_json)
            feature_histograms.append(feature_histogram)
        return feature_histograms