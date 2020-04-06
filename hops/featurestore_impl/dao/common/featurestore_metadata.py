from hops import constants
from hops.featurestore_impl.dao.datasets.training_dataset import TrainingDataset
from hops.featurestore_impl.dao.featuregroups.featuregroup import Featuregroup
from hops.featurestore_impl.dao.featurestore.featurestore import Featurestore
from hops.featurestore_impl.dao.settings.featurestore_settings import FeaturestoreSettings
from hops.featurestore_impl.dao.storageconnectors.hopsfs_connector import HopsfsStorageConnector
from hops.featurestore_impl.dao.storageconnectors.jdbc_connector import JDBCStorageConnector
from hops.featurestore_impl.dao.storageconnectors.s3_connector import S3StorageConnector
from hops.featurestore_impl.util import fs_utils

class FeaturestoreMetadata(object):
    """
    Represents feature store metadata. This metadata is used by the feature store client to determine how to
    fetch and push features from/to the feature store
    """

    def __init__(self, metadata_json):
        """
        Initialize the featurestore metadata from JSON payload

        Args:
            :metadata_json: JSON metadata about the featurestore returned from Hopsworks REST API
        """
        featuregroups, training_datasets, features_to_featuregroups, featurestore, settings, storage_connectors, \
            online_featurestore_connector = self._parse_featurestore_metadata(metadata_json)
        self.featuregroups = featuregroups
        self.training_datasets = training_datasets
        self.features_to_featuregroups = features_to_featuregroups
        self.featurestore = featurestore
        self.settings = settings
        self.storage_connectors = storage_connectors
        constants.FEATURE_STORE.TRAINING_DATASET_SUPPORTED_FORMATS = settings.training_dataset_formats
        self.online_featurestore_connector = online_featurestore_connector


    def _parse_featurestore_metadata(self, metadata_json):
        """
        Parses the featurestore metadata from the REST API and puts it into an optimized data structure
        with O(1) lookup time for features, featuregroups, and training datasets

        Args:
            :featurestore_metadata: the JSON metadata of the featurestore returned by hopsworks

        Returns:
            the parsed metadata

        """
        featuregroups = {}
        training_datasets = {}
        features_to_featuregroups = {}
        storage_connectors = {}
        for fg in metadata_json[constants.REST_CONFIG.JSON_FEATUREGROUPS]:
            fg_obj = Featuregroup(fg)
            featuregroups[fs_utils._get_table_name(fg[constants.REST_CONFIG.JSON_FEATUREGROUP_NAME],
                                                     fg[constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION])] = fg_obj
            for f in fg[constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES]:
                if f[constants.REST_CONFIG.JSON_FEATURE_NAME] in features_to_featuregroups:
                    features_to_featuregroups[f[constants.REST_CONFIG.JSON_FEATURE_NAME]].append(fg_obj)
                else:
                    features_to_featuregroups[f[constants.REST_CONFIG.JSON_FEATURE_NAME]] = [fg_obj]
        for td in metadata_json[constants.REST_CONFIG.JSON_TRAINING_DATASETS]:
            training_datasets[fs_utils._get_table_name(td[constants.REST_CONFIG.JSON_TRAINING_DATASET_NAME],
                                                         td[constants.REST_CONFIG.JSON_TRAINING_DATASET_VERSION])] = \
                TrainingDataset(td)

        settings = FeaturestoreSettings(metadata_json[constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS])
        for sc in metadata_json[constants.REST_CONFIG.JSON_FEATURESTORE_STORAGE_CONNECTORS]:
            if sc[constants.REST_CONFIG.JSON_FEATURESTORE_CONNECTOR_TYPE] == \
                    settings.jdbc_connector_type:
                storage_connectors[sc[constants.REST_CONFIG.JSON_FEATURESTORE_CONNECTOR_NAME]] = \
                    JDBCStorageConnector(sc)
            if sc[constants.REST_CONFIG.JSON_FEATURESTORE_CONNECTOR_TYPE] == \
                    settings.s3_connector_type:
                storage_connectors[sc[constants.REST_CONFIG.JSON_FEATURESTORE_CONNECTOR_NAME]] = S3StorageConnector(sc)
            if sc[constants.REST_CONFIG.JSON_FEATURESTORE_CONNECTOR_TYPE] == \
                    settings.hopsfs_connector_type:
                storage_connectors[sc[constants.REST_CONFIG.JSON_FEATURESTORE_CONNECTOR_NAME]] = \
                    HopsfsStorageConnector(sc)
        featurestore = Featurestore(metadata_json[constants.REST_CONFIG.JSON_FEATURESTORE])
        if constants.REST_CONFIG.JSON_FEATURESTORE_ONLINE_CONNECTOR in metadata_json:
            online_featurestore_connector = JDBCStorageConnector(
                metadata_json[constants.REST_CONFIG.JSON_FEATURESTORE_ONLINE_CONNECTOR])
        else:
            online_featurestore_connector = None
        return featuregroups, training_datasets, features_to_featuregroups, \
               featurestore, settings, storage_connectors, online_featurestore_connector
