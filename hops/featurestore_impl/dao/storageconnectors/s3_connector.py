from hops import constants

class S3StorageConnector():
    """
    Represents a s3 storage connector in the feature store
    """

    def __init__(self, s3_storage_connector_json):
        """
        Initialize the s3 connector from JSON payload

        Args:
            :s3_storage_connector_json: JSON representation of the storage connector, returned from Hopsworks REST API
        """
        self.name = s3_storage_connector_json[constants.REST_CONFIG.JSON_FEATURESTORE_CONNECTOR_NAME]
        self.id = s3_storage_connector_json[constants.REST_CONFIG.JSON_FEATURESTORE_CONNECTOR_ID]
        self.description = s3_storage_connector_json[constants.REST_CONFIG.JSON_FEATURESTORE_CONNECTOR_DESCRIPTION]
        self.type = s3_storage_connector_json[constants.REST_CONFIG.JSON_FEATURESTORE_CONNECTOR_TYPE]
        self.featurestore_id = \
            s3_storage_connector_json[constants.REST_CONFIG.JSON_FEATURESTORE_CONNECTOR_FEATURESTORE_ID]
        self.bucket = s3_storage_connector_json[constants.REST_CONFIG.JSON_FEATURESTORE_S3_BUCKET]

        if constants.REST_CONFIG.JSON_FEATURESTORE_S3_ACCESS_KEY in s3_storage_connector_json: 
            self.access_key = s3_storage_connector_json[constants.REST_CONFIG.JSON_FEATURESTORE_S3_ACCESS_KEY]
        else: 
            self.access_key = None 

        if constants.REST_CONFIG.JSON_FEATURESTORE_S3_SECRET_KEY in s3_storage_connector_json: 
            self.secret_key = s3_storage_connector_json[constants.REST_CONFIG.JSON_FEATURESTORE_S3_SECRET_KEY]
        else:
            self.secret_key = None

    