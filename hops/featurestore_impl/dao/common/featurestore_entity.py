from hops.featurestore_impl.dao.features.feature import Feature

class FeaturestoreEntity(object):
    """
    Represents an abstract featurestore entity (contains common functionality between feature groups and
    training datasets in the featurestore
    """

    def _parse_features(self, features_json):
        """
        Parses a list of features in JSON format into a list of Feature objects

        Args:
            :features_json: json representation of the list of features

        Returns:
            a list of Feature objects
        """
        return list(map(lambda feature_json: Feature(feature_json), features_json))