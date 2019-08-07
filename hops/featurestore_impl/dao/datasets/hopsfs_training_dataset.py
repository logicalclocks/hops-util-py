from hops import constants

class HopsfsTrainingDataset():
    """
    Represents a training dataset on hopsfs in the feature store
    """

    def __init__(self, hopsfs_training_dataset_json):
        """
        Initalizes the hopsfs training dataset from JSON payload

        Args:
            :external_training_dataset_json: JSON data about the training dataset returned from Hopsworks REST API
        """
        self.hopsfs_connector_id = hopsfs_training_dataset_json[
            constants.REST_CONFIG.JSON_TRAINING_DATASET_HOPSFS_CONNECTOR_ID]
        self.hopsfs_connector_name = hopsfs_training_dataset_json[
            constants.REST_CONFIG.JSON_TRAINING_DATASET_HOPSFS_CONNECTOR_NAME]
        self.size = hopsfs_training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_SIZE]
        self.hdfs_store_path = hopsfs_training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_HDFS_STORE_PATH]
        self.inode_id = hopsfs_training_dataset_json[constants.REST_CONFIG.JSON_TRAINING_DATASET_INODE_ID]


