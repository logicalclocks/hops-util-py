from hops import constants
from hops.featurestore_impl.dao.stats.histogram_bin import HistogramBin


class FeatureHistogram(object):
    """
    Represents a FeatureHistogram for a feature in a feature group or training dataset
    """

    def __init__(self, feature_histogram_json):
        """
        Initialize the feature histogram object from JSON payload

        Args:
            :feature_histogram_json: JSON data of the feature histogram
        """
        self.feature_name = feature_histogram_json[constants.REST_CONFIG.JSON_HISTOGRAM_FEATURE_NAME]
        self.frequency_distribution = self._parse_frequency_distribution(feature_histogram_json)


    def _parse_frequency_distribution(self, feature_histogram_json):
        """
        Parse list of histogram bins in a feature histogram JSON payload

        Args:
            :feature_histogram_json: the JSON payload with the feature histogram

        Returns:
            A list of the parsed histogram bins
        """
        frequency_distribution = []
        for histogram_bin_json in \
                feature_histogram_json[constants.REST_CONFIG.JSON_HISTOGRAM_FREQUENCY_DISTRIBUTION]:
            histogram_bin = HistogramBin(histogram_bin_json)
            frequency_distribution.append(histogram_bin)
        return frequency_distribution