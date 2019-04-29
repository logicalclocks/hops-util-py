from hops.featurestore_impl.query_planner.fs_query import FeaturestoreQuery
from hops import constants

class FeaturesQuery(FeaturestoreQuery):
    """
    Represents a user query for a list of features from the featurestore
    """

    def __init__(self, features, featurestore_metadata, featurestore, featuregroups_version_dict, join_key):
        """
        Initializes the query object from user-supplied data

        Args:
            :features: the list of features to get
            :featurestore_metadata: the metadata of the featurestore to query
            :featurestore: the featurestore to query
            :featuregroups_version_dict: a dict of featuregroups and their versions to get the features from (optional)
            :join_key: the join key to join features together (optional)
        """
        self.featurestore = self._default_featurestore(featurestore)
        self.features = list(set(features))
        self.featurestore_metadata = featurestore_metadata
        self.featuregroups_version_dict_orig = featuregroups_version_dict
        self.featuregroups_version_dict = self._convert_featuregroup_version_dict(featuregroups_version_dict)
        self.join_key = join_key

    def _convert_featuregroup_version_dict(self, featuregroups_version_dict):
        """
        Converts a featuregroup->version dict into a list of {name: name, version: version}

        Args:
            :featuregroups_version_dict: a dict of featuregroup --> version provided by the user

        Returns:
            a list of {name: name, version: version}

        """
        parsed_featuregroups = []
        for i, (name, version) in enumerate(featuregroups_version_dict.items()):
            parsed_featuregroup = {}
            parsed_featuregroup[constants.REST_CONFIG.JSON_FEATUREGROUP_NAME] = name
            parsed_featuregroup[constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION] = version
            parsed_featuregroups.append(parsed_featuregroup)
        return parsed_featuregroups

class FeatureQuery(FeaturestoreQuery):
    """
    Represents a user-query for a single feature in the featurestore
    """

    def __init__(self, feature, featurestore_metadata, featurestore, featuregroup, featuregroup_version):
        """
        Initializes the query object with user-supplied data

        Args:
            :feature: the feature to get from the featurestore
            :featurestore_metadata: the metadata of the featurestore
            :featurestore: the featurestore to query
            :featuregroup: the featuregroup where the feature resides (optional)
            :featuregroup_version: the version of the featuregroup (defaults to 1)
        """
        self.feature = feature
        self.featurestore_metadata = featurestore_metadata
        self.featurestore = self._default_featurestore(featurestore)
        self.featuregroup = featuregroup
        self.featuregroup_version = featuregroup_version
