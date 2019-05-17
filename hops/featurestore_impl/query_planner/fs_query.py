from hops.featurestore_impl.util import fs_utils


class FeaturestoreQuery(object):
    """
    Represents an abstract query to the feature store
    """

    def _default_featurestore(self, featurestore):
        """
        Returns the default featurestore. If the user did not specify the featurestore, it defaults to the project's
        featurestore

        Args:
            :featurestore: the featurestore argument provided by the user

        Returns:
            The user provided featurestore if not None, otherwise the project's featurestore
        """
        if featurestore is None:
            featurestore = fs_utils._do_get_project_featurestore()
        return featurestore