from hops import constants

class HistogramBin(object):
    """
    Represents a Histogram Bin in a Feature Histogram of a feature group or training dataset
    """

    def __init__(self, histogram_bin_json):
        """
        Initialize the histogram bin object from JSON payload

        Args:
            :histogram_bin_json: JSON data of the histogram bin
        """
        self.bin = histogram_bin_json[constants.REST_CONFIG.JSON_HISTOGRAM_BIN]
        self.frequency = int(histogram_bin_json[constants.REST_CONFIG.JSON_HISTOGRAM_FREQUENCY])