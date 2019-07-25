from hops import constants
from hops.featurestore_impl.query_planner.f_query import FeaturesQuery, FeatureQuery
from hops.featurestore_impl.query_planner.fg_query import FeaturegroupQuery
from hops.featurestore_impl.exceptions.exceptions import FeaturegroupNotFound
from hops.featurestore_impl.util import fs_utils
from hops.featurestore_impl.query_planner import query_planner

class LogicalQueryPlan(object):
    """
    Represents a logical query plan for getting features from the feature store hive database
    """

    def __init__(self, fs_query):
        """
        Initializes the query plan from a feature store query

        Args:
            :fs_query: the user query
        """
        self.query = fs_query

    def create_logical_plan(self):
        """
        Creates a logical plan from a user query

        Returns:
            None
        """
        if isinstance(self.query, FeatureQuery):
            self._feature_query()
        if isinstance(self.query, FeaturesQuery):
            self._features_query()
        if isinstance(self.query, FeaturegroupQuery):
            self._featuregroup_query()

    def construct_sql(self):
        """
        Constructs a HiveSQL query from the logical plan

        Returns:
            None
        """
        sql_str = "SELECT " + self.features_str + " FROM " + self.featuregroups_str
        if self.join_str is not None:
            sql_str = sql_str + " " + self.join_str
        self.sql_str = sql_str
        fs_utils._log("SQL string for the query created successfully")

    def _feature_query(self):
        """
        Creates a logical query plan for a user-query for a single feature

        Returns:
            None

        Raises:
            :FeaturegroupNotFound: if the feature could not be found in any of the featuregroups in the metadata
        """
        self.join_str = None
        self.features_str = self.query.feature
        if self.query.featuregroup != None:
            self.featuregroups_str =  fs_utils._get_table_name(self.query.featuregroup,
                                                                 self.query.featuregroup_version)
            self.featuregroups = [self.query.featurestore_metadata.featuregroups[
                                 fs_utils._get_table_name(self.query.featuregroup,
                                                          self.query.featuregroup_version)
                             ]]
        else:
            featuregroups_parsed = self.query.featurestore_metadata.featuregroups
            if len(featuregroups_parsed.values()) == 0:
                raise FeaturegroupNotFound("Could not find any featuregroups in the metastore "
                                           "that contains the given feature, "
                                           "please explicitly supply featuregroups as an argument to the API call")
            featuregroup_matched = query_planner._find_feature(self.query.feature, self.query.featurestore,
                                                               featuregroups_parsed.values())
            self.featuregroups_str = fs_utils._get_table_name(featuregroup_matched.name, featuregroup_matched.version)
            self.featuregroups = [featuregroup_matched]

        fs_utils._log("Logical query plan for getting 1 feature from the featurestore created successfully")

    def _features_query(self):
        """
        Creates a logical query plan from a user query to get a list of features

        Returns:
            None

        Raises:
            :FeaturegroupNotFound: if the some of the features could not be found in any of the featuregroups
        """
        self.features_str = ", ".join(self.query.features)
        self.join_str = None

        if len(self.query.featuregroups_version_dict) == 1:
            self.featuregroups_str = fs_utils._get_table_name(
                self.query.featuregroups_version_dict[0][constants.REST_CONFIG.JSON_FEATUREGROUP_NAME],
                self.query.featuregroups_version_dict[0][constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION]
            )
            featuregroups = [self.query.featurestore_metadata.featuregroups[
                                 fs_utils._get_table_name(
                                     self.query.featuregroups_version_dict[0][constants.REST_CONFIG.JSON_FEATUREGROUP_NAME],
                                     self.query.featuregroups_version_dict[0][constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION]
                                 )
                             ]]
            self.featuregroups = featuregroups

        if len(self.query.featuregroups_version_dict) > 1:
            if self.query.join_key != None:
                featuregroups = [self.query.featurestore_metadata.featuregroups[
                                     fs_utils._get_table_name(
                                         entry[constants.REST_CONFIG.JSON_FEATUREGROUP_NAME],
                                         entry[constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION]
                                     )
                                 ]
                                 for entry in self.query.featuregroups_version_dict]
                self.join_str = query_planner._get_join_str(featuregroups, self.query.join_key)
                self.featuregroups_str = fs_utils._get_table_name(featuregroups[0].name,
                                                                  featuregroups[0].version)
                self.featuregroups = featuregroups

            else:
                featuregroups_parsed = self.query.featurestore_metadata.featuregroups
                if len(featuregroups_parsed.values()) == 0:
                    raise FeaturegroupNotFound("Could not find any featuregroups containing "
                                               "the features in the metastore, " 
                                               "please explicitly supply featuregroups as an argument to the API call")
                featuregroups_filtered = list(filter(lambda fg: fg.name in self.query.featuregroups_version_dict_orig
                                                                and self.query.featuregroups_version_dict_orig[fg.name]
                                                                    == fg.version, featuregroups_parsed.values()))
                join_col = query_planner._get_join_col(featuregroups_filtered)
                self.join_str = query_planner._get_join_str(featuregroups_filtered, join_col)
                self.featuregroups_str = fs_utils._get_table_name(featuregroups_filtered[0].name,
                                                                  featuregroups_filtered[0].version)
                self.featuregroups = featuregroups_filtered

        if len(self.query.featuregroups_version_dict) == 0:
            featuregroups_parsed = self.query.featurestore_metadata.featuregroups
            if len(featuregroups_parsed.values()) == 0:
                raise FeaturegroupNotFound("Could not find any featuregroups in the metastore, "
                                           "please explicitly supply featuregroups as an argument to the API call")
            feature_to_featuregroup = {}
            feature_featuregroups = []
            for feature in self.query.features:
                featuregroup_matched = query_planner._find_feature(feature, self.query.featurestore,
                                                                   featuregroups_parsed.values())
                feature_to_featuregroup[feature] = featuregroup_matched
                if not query_planner._check_if_list_of_featuregroups_contains_featuregroup(
                        feature_featuregroups,featuregroup_matched.name,
                        featuregroup_matched.version):
                    feature_featuregroups.append(featuregroup_matched)

            if len(feature_featuregroups) == 1:
                self.featuregroups_str = fs_utils._get_table_name(feature_featuregroups[0].name,
                                                                    feature_featuregroups[0].version)
            else:
                join_col = query_planner._get_join_col(feature_featuregroups)
                self.join_str = query_planner._get_join_str(feature_featuregroups, join_col)
                self.featuregroups_str = fs_utils._get_table_name(feature_featuregroups[0].name,
                                                                    feature_featuregroups[0].version)
            self.featuregroups = feature_featuregroups

        fs_utils._log(
            "Logical query plan for getting {} features from the featurestore created successfully".format(
                len(self.query.features)))


    def _featuregroup_query(self):
        """
        Creates a logical query plan for a user query to get a featuregroup

        Returns:
            None

        """
        self.features_str = "*"
        self.join_str = None
        self.featuregroups_str = fs_utils._get_table_name(self.query.featuregroup,
                                                            self.query.featuregroup_version)