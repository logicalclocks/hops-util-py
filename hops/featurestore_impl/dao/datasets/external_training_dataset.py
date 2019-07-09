from hops import constants

class ExternalTrainingDataset():
    """
    Represents an external training dataset in the feature store
    """

    def __init__(self, external_training_dataset_json):
        """
        Initalizes the external training dataset from JSON payload

        Args:
            :external_training_dataset_json: JSON data about the training dataset returned from Hopsworks REST API
        """
        self.s3_connector_id = external_training_dataset_json[
            constants.REST_CONFIG.JSON_TRAINING_DATASET_S3_CONNECTOR_ID]
        self.s3_connector_name = external_training_dataset_json[
            constants.REST_CONFIG.JSON_TRAINING_DATASET_S3_CONNECTOR_NAME]


