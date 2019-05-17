from hops import constants
from hops.featurestore_impl.dao.training_dataset import TrainingDataset
from hops.featurestore_impl.dao.featuregroup import Featuregroup
from hops.featurestore_impl.dao.featurestore import Featurestore
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
        featuregroups, training_datasets, features_to_featuregroups, featurestore = \
            self._parse_featurestore_metadata(metadata_json)
        self.featuregroups = featuregroups
        self.training_datasets = training_datasets
        self.features_to_featuregroups = features_to_featuregroups
        self.featurestore = featurestore


    def _parse_featurestore_metadata(self, metadata_json):
        """
        Parses the featurestore metadata from the REST API and puts it into an optimized data structure
        with O(1) lookup time for features, featuregroups, and training datasets

        Args:
            :featurestore_metadata: the JSON metadata of the featurestore returned by hopsworks

        Returns:
            A dict with parsed metadata

        """
        featuregroups = {}
        training_datasets = {}
        features_to_featuregroups = {}
        for fg in metadata_json[constants.REST_CONFIG.JSON_FEATUREGROUPS]:
            featuregroups[fs_utils._get_table_name(fg[constants.REST_CONFIG.JSON_FEATUREGROUP_NAME],
                                                     fg[constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION])] = \
                Featuregroup(fg)
            for f in fg[constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES]:
                if f[constants.REST_CONFIG.JSON_FEATURE_NAME] in features_to_featuregroups:
                    features_to_featuregroups[f[constants.REST_CONFIG.JSON_FEATURE_NAME]].append(Featuregroup(fg))
                else:
                    features_to_featuregroups[f[constants.REST_CONFIG.JSON_FEATURE_NAME]] = [Featuregroup(fg)]
        for td in metadata_json[constants.REST_CONFIG.JSON_TRAINING_DATASETS]:
            training_datasets[fs_utils._get_table_name(td[constants.REST_CONFIG.JSON_TRAINING_DATASET_NAME],
                                                         td[constants.REST_CONFIG.JSON_TRAINING_DATASET_VERSION])] = \
                TrainingDataset(td)
        featurestore = Featurestore(metadata_json[constants.REST_CONFIG.JSON_FEATURESTORE])
        return featuregroups, training_datasets, features_to_featuregroups, featurestore
