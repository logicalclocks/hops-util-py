from hops.featurestore_impl.dao.stats.correlation_matrix import CorrelationMatrix
from hops.featurestore_impl.dao.stats.descriptive_stats import DescriptiveStats
from hops.featurestore_impl.dao.stats.feature_histograms import FeatureHistograms

from hops import constants
from hops.featurestore_impl.dao.stats.cluster_analysis import ClusterAnalysis


class Statistics(object):
    """
    Represents statistics computed for a featuregroup or training dataset in the featurestore
    """

    def __init__(self, descriptive_stats_json, correlation_matrix_json, features_histogram_json, cluster_analysis_json):
        """
        Initialize the statistics object from JSON payload

        Args:
            :descriptive_stats_json: JSON data of the descriptive statistics
            :correlation_matrix_json: JSON data of the feature correlations
            :features_histogram_json: JSON data of the features histograms
            :cluster_analysis_json: JSON data of feature cluster analysis
        """
        if descriptive_stats_json is not None:
            self.descriptive_stats = DescriptiveStats(descriptive_stats_json)
        else:
            self.descriptive_stats = None
        if correlation_matrix_json is not None:
            self.correlation_matrix = CorrelationMatrix(correlation_matrix_json)
        else:
            self.correlation_matrix = None
        if features_histogram_json is not None and \
            features_histogram_json[constants.REST_CONFIG.JSON_HISTOGRAM_FEATURE_DISTRIBUTIONS] is not None:
            self.feature_histograms = \
                FeatureHistograms(features_histogram_json[constants.REST_CONFIG.JSON_HISTOGRAM_FEATURE_DISTRIBUTIONS])
        else:
            self.feature_histograms = None
        if cluster_analysis_json is not None:
            self.cluster_analysis = ClusterAnalysis(cluster_analysis_json)
        else:
            self.cluster_analysis = None
