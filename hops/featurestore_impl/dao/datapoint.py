from hops import constants

class DataPoint(object):
    """
    Represents a Data Point in Cluster Analysis computed for a featuregroup or training dataset in the featurestore
    """

    def __init__(self, data_point_json):
        """
        Initialize the data point object from JSON payload

        Args:
            :data_point_json: JSON data of the data point
        """
        self.name = data_point_json[constants.REST_CONFIG.JSON_CLUSTERING_ANALYSIS_DATA_POINT_NAME]
        self.first_dimension = data_point_json[constants.REST_CONFIG.JSON_CLUSTERING_ANALYSIS_FIRST_DIMENSION]
        self.second_dimension = data_point_json[constants.REST_CONFIG.JSON_CLUSTERING_ANALYSIS_SECOND_DIMENSION]