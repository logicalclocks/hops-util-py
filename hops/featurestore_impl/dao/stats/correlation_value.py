from hops import constants

class CorrelationValue(object):
    """
    Represents a correlation value in a correlation matrix
    """

    def __init__(self, correlation_value_json):
        """
        Initialize the correlation value object from JSON payload

        Args:
            :correlation_value_json: JSON payload with the correlation value
        """
        self.feature_name = correlation_value_json[constants.REST_CONFIG.JSON_CORRELATION_FEATURE_NAME]
        self.correlation = float(correlation_value_json[constants.REST_CONFIG.JSON_CORRELATION])