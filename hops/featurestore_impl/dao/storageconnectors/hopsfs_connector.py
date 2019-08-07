from hops import constants

class HopsfsStorageConnector():
    """
    Represents a HopsFS storage connector in the feature store
    """

    def __init__(self, hopsfs_storage_connector_json):
        """
        Initialize the hopsfs connector from JSON payload

        Args:
            :hopsfs_storage_connector_json: JSON representation of the storage connector,
                                            returned from Hopsworks REST API
        """
        self.name = hopsfs_storage_connector_json[constants.REST_CONFIG.JSON_FEATURESTORE_CONNECTOR_NAME]
        self.id = hopsfs_storage_connector_json[constants.REST_CONFIG.JSON_FEATURESTORE_CONNECTOR_ID]
        self.description = hopsfs_storage_connector_json[constants.REST_CONFIG.JSON_FEATURESTORE_CONNECTOR_DESCRIPTION]
        self.type = hopsfs_storage_connector_json[constants.REST_CONFIG.JSON_FEATURESTORE_CONNECTOR_TYPE]
        self.featurestore_id = \
            hopsfs_storage_connector_json[constants.REST_CONFIG.JSON_FEATURESTORE_CONNECTOR_FEATURESTORE_ID]
        self.hopsfs_path = \
            hopsfs_storage_connector_json[constants.REST_CONFIG.JSON_FEATURESTORE_HOPSFS_CONNECTOR_HOPSFS_PATH]
        self.dataset_name = \
            hopsfs_storage_connector_json[constants.REST_CONFIG.JSON_FEATURESTORE_HOPSFS_CONNECTOR_DATASET_NAME]