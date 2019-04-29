from hops.featurestore_impl.dao.feature import Feature
from hops.featurestore_impl.dao.featurestore_dependency import FeaturestoreDependency

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


    def _parse_dependencies(self, dependencies_json):
        """
        Parses a list of featurestore dependnecies in JSON format into a list of FeaturestoreDependency objects

        Args:
            :dependencies_json: json representation of the list of dependencies

        Returns:
            a list of FeaturestoreDependency objects
        """
        return list(map(lambda dependency_json: FeaturestoreDependency(dependency_json), dependencies_json))