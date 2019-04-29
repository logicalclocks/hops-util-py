from hops.featurestore_impl.query_planner.fs_query import FeaturestoreQuery

class FeaturegroupQuery(FeaturestoreQuery):
    """
    Represents a user query to get a feature group from the feature store
    """

    def __init__(self, featuregroup, featurestore, featuregroup_version):
        """
        Initializes the query object from user-supplied data

        Args:
            :featuregroup: the featuregroup to get
            :featurestore: the featurestore to query
            :featuregroup_version: the version of the featuregroup (defaults to 1)
        """
        self.featuregroup = featuregroup
        self.featurestore = self._default_featurestore(featurestore)
        self.featuregroup_version = featuregroup_version