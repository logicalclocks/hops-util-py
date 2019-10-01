from hops import constants

class JDBCStorageConnector():
    """
    Represents a JDBC storage connector in the feature store
    """

    def __init__(self, jdbc_storage_connector_json):
        """
        Initialize the jdbc connector from JSON payload

        Args:
            :jdbc_storage_connector_json: JSON representation of the storage connector, returned from Hopsworks REST API
        """
        self.name = jdbc_storage_connector_json[constants.REST_CONFIG.JSON_FEATURESTORE_CONNECTOR_NAME]
        if constants.REST_CONFIG.JSON_FEATURESTORE_CONNECTOR_ID in jdbc_storage_connector_json:
            self.id = jdbc_storage_connector_json[constants.REST_CONFIG.JSON_FEATURESTORE_CONNECTOR_ID]
        else:
            self.id = -1
        self.description = jdbc_storage_connector_json[constants.REST_CONFIG.JSON_FEATURESTORE_CONNECTOR_DESCRIPTION]
        self.type = jdbc_storage_connector_json[constants.REST_CONFIG.JSON_FEATURESTORE_CONNECTOR_TYPE]
        self.featurestore_id = \
            jdbc_storage_connector_json[constants.REST_CONFIG.JSON_FEATURESTORE_CONNECTOR_FEATURESTORE_ID]
        self.connection_string = \
            jdbc_storage_connector_json[constants.REST_CONFIG.JSON_FEATURESTORE_JDBC_CONNECTOR_CONNECTION_STRING]
        if constants.REST_CONFIG.JSON_FEATURESTORE_JDBC_CONNECTOR_ARGUMENTS in jdbc_storage_connector_json:
            self.arguments = \
                jdbc_storage_connector_json[constants.REST_CONFIG.JSON_FEATURESTORE_JDBC_CONNECTOR_ARGUMENTS]
        else:
            self.arguments = []