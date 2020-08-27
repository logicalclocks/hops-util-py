from hops import constants

import re

class FeaturestoreSettings():
    """
    Represents a feature store settings
    """

    def __init__(self, settings_json):
        """
        Initalizes the settings from the JSON payload

        Args:
            :settings_json: JSON data about the settings returned from Hopsworks REST API
        """
        self.entity_name_max_len = \
            settings_json[constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_ENTITY_NAME_MAX_LENGTH]
        self.entity_description_max_len = \
            settings_json[constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_ENTITY_DESCRIPTION_MAX_LENGTH]
        self.external_training_dataset_type = \
            settings_json[constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_EXTERNAL_TRAINING_DATASET_TYPE]
        self.featurestore_regex = re.compile(
            settings_json[constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_FEATURESTORE_REGEX])
        self.hopsfs_connector_dto_type = \
            settings_json[constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_HOPSFS_CONNECTOR_DTO_TYPE]
        self.hopsfs_connector_type = \
            settings_json[constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_HOPSFS_CONNECTOR_TYPE]
        self.hopsfs_training_dataset_type = \
            settings_json[constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_HOPSFS_TRAINING_DATASET_TYPE]
        self.jdbc_connector_dto_type = \
            settings_json[constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_JDBC_CONNECTOR_DTO_TYPE]
        self.jdbc_connector_type = settings_json[constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_JDBC_CONNECTOR_TYPE]
        self.jdbc_connector_arguments_max_len = \
            settings_json[constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_JDBC_CONNECTOR_ARGUMENTS_MAX_LEN]
        self.jdbc_connector_connection_str_max_len = \
            settings_json[
                constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_JDBC_CONNECTOR_CONNECTION_STRING_MAX_LEN]
        self.on_demand_Featuregroup_sql_query_max_len = \
            settings_json[constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_ON_DEMAND_FEATUREGROUP_SQL_QUERY_MAX_LEN]
        self.s3_connector_dto_type = \
            settings_json[constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_S3_CONNECTOR_DTO_TYPE]
        self.s3_connector_type = \
            settings_json[constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_S3_CONNECTOR_TYPE]
        self.s3_connector_access_key_max_len = \
            settings_json[constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_S3_CONNECTOR_ACCESS_KEY_MAX_LEN]
        self.s3_connector_bucket_max_len = \
            settings_json[constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_S3_CONNECTOR_BUCKET_MAX_LEN]
        self.s3_connector_secret_key_max_len = \
            settings_json[constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_S3_CONNECTOR_SECRET_KEY_MAX_LEN]
        self.storage_connector_desc_max_len = \
            settings_json[constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_STORAGE_CONNECTOR_DESCRIPTION_MAX_LEN]
        self.storage_connector_max_len = \
            settings_json[constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_STORAGE_CONNECTOR_NAME_MAX_LEN]
        self.suggested_hive_feature_types = \
            settings_json[constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_HIVE_SUGGESTED_FEATURE_TYPES]
        self.suggested_mysql_feature_types = \
            settings_json[constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_MYSQL_SUGGESTED_FEATURE_TYPES]
        self.training_dataset_formats = \
            settings_json[constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_TRAINING_DATASET_DATA_FORMATS]
        self.training_dataset_type = \
            settings_json[constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_TRAINING_DATASET_TYPE]
        self.feature_import_connectors = \
            settings_json[constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_IMPORT_CONNECTORS]
        self.online_enabled = \
            settings_json[constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_ONLINE_ENABLED]
