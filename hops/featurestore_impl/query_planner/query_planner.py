"""
Query Planner functions for inferring how to perform user queries to the featurestore
"""
from hops import constants
from hops.featurestore_impl.query_planner.f_query import FeaturesQuery, FeatureQuery
from hops.featurestore_impl.query_planner.fg_query import FeaturegroupQuery
from hops.featurestore_impl.util import fs_utils
from hops.featurestore_impl.exceptions.exceptions import FeatureNotFound, FeatureNameCollisionError, InferJoinKeyError, \
    TrainingDatasetNotFound, FeaturegroupNotFound


def _find_featuregroup_that_contains_feature(featuregroups, feature):
    """
    Go through list of featuregroups and find the ones that contain the feature

    Args:
        :featuregroups: featuregroups to search through
        :feature: the feature to look for

    Returns:
        a list of featuregroup names and versions for featuregroups that contain the given feature

    """
    matches = []
    for fg in featuregroups:
        for f in fg.features:
            fg_table_name = fs_utils._get_table_name(fg.name, fg.version)
            full_name = fg_table_name + constants.DELIMITERS.DOT_DELIMITER + f.name
            if f.name == feature or full_name == feature or (f.name in feature and fg.name in feature):
                matches.append(fg)
                break
    return matches


def _find_feature(feature, featurestore, featuregroups_parsed):
    """
    Looks if a given feature can be uniquely found in a list of featuregroups and returns that featuregroup.
    Otherwise it throws an exception

    Args:
        :feature: the feature to search for
        :featurestore: the featurestore where the featuregroups resides
        :featuregroups_parsed: the featuregroups to look through

    Returns:
        the featuregroup that contains the feature

    Raises:
        :FeatureNotFound: if the requested feature could not be found
    """
    featuregroups_matched = _find_featuregroup_that_contains_feature(featuregroups_parsed, feature)
    if (len(featuregroups_matched) == 0):
        raise FeatureNotFound(
            "Could not find the feature with name '{}' in any of the featuregroups of the featurestore: '{}'".format(
                feature, featurestore))
    if (len(featuregroups_matched) > 1):
        featuregroups_matched_str_list = map(lambda fg: fs_utils._get_table_name(fg.name, fg.version),
                                             featuregroups_matched)
        featuregroups_matched_str = ",".join(featuregroups_matched_str_list)
        raise FeatureNameCollisionError("Found the feature with name '{}' "
                             "in more than one of the featuregroups of the featurestore: '{}', "
                             "please specify the optional argument 'featuregroup=', "
                             "the matched featuregroups were: {}".format(feature, featurestore,
                                                                         featuregroups_matched_str))
    return featuregroups_matched[0]


def _get_join_str(featuregroups, join_key):
    """
    Constructs the JOIN COl,... ON X string from a list of tables (featuregroups) and join column
    Args:
        :featuregroups: the featuregroups to join
        :join_key: the key to join on

    Returns:
        SQL join string to join a set of feature groups together
    """
    join_str = ""
    for idx, fg in enumerate(featuregroups):
        if (idx != 0):
            join_str = join_str + "JOIN " + fs_utils._get_table_name(fg.name, fg.version) + " "
    join_str = join_str + "ON "
    for idx, fg in enumerate(featuregroups):
        if (idx != 0 and idx < (len(featuregroups) - 1)):
            join_str = join_str + fs_utils._get_table_name(featuregroups[0].name, featuregroups[0].version) + ".`" \
                       + join_key + "`=" + \
                       fs_utils._get_table_name(fg.name, fg.version) + ".`" + join_key \
                       + "` AND "
        elif (idx != 0 and idx == (len(featuregroups) - 1)):
            join_str = join_str + fs_utils._get_table_name(featuregroups[0].name, featuregroups[0].version) + ".`" \
                       + join_key + "`=" + \
                       fs_utils._get_table_name(fg.name, fg.version) + ".`" + join_key + "`"
    return join_str


def _get_col_that_is_primary(common_cols, featuregroups):
    """
    Helper method that returns the column among a shared column between featuregroups that is most often marked as
    'primary' in the hive schema.

    Args:
        :common_cols: the list of columns shared between all featuregroups
        :featuregroups: the list of featuregroups

    Returns:
        the column among a shared column between featuregroups that is most often marked as 'primary' in the hive schema
    """
    primary_counts = []
    for col in common_cols:
        primary_count = 0
        for fg in featuregroups:
            for feature in fg.features:
                if feature.name == col and feature.primary:
                    primary_count = primary_count + 1
        primary_counts.append(primary_count)

    max_no_primary = max(primary_counts)

    if max(primary_counts) == 0:
        return common_cols[0]
    else:
        return common_cols[primary_counts.index(max_no_primary)]


def _get_join_col(featuregroups):
    """
    Finds a common JOIN column among featuregroups (hive tables)

    Args:
        :featuregroups: a list of featuregroups with version and features

    Returns:
        name of the join column

    Raises:
        :InferJoinKeyError: if the join key of the featuregroups could not be inferred automatically
    """
    feature_sets = []
    for fg in featuregroups:
        columns = fg.features
        columns_names = map(lambda col: col.name, columns)
        feature_set = set(columns_names)
        feature_sets.append(feature_set)
    common_cols = list(set.intersection(*feature_sets))

    if (len(common_cols) == 0):
        featuregroups_str = ", ".join(
            list(map(lambda fg: fg.name, featuregroups)))
        raise InferJoinKeyError("Could not find any common columns in featuregroups to join on, " \
                             "searched through featuregroups: " \
                             "{}".format(featuregroups_str))
    return _get_col_that_is_primary(common_cols, featuregroups)


def _check_if_list_of_featuregroups_contains_featuregroup(featuregroups, featuregroup_name, version):
    """
    Check if a list of featuregroup contains a featuregroup with a particular name and version

    Args:
        :featuregroups: the list of featuregroups to search through
        :featuregroup_name: the name of the featuregroup
        :version: the featuregroup version

    Returns:
        boolean indicating whether the featuregroup name and version exists in the list
    """
    match_bool = False
    for fg in featuregroups:
        if (fg.name == featuregroup_name and fg.version == version):
            match_bool = True
    return match_bool


def _find_training_dataset(training_datasets, training_dataset, training_dataset_version):
    """
    A helper function to look for a training dataset name and version in a list of training datasets

    Args:
        :training_datasets: a list of training datasets metadata
        :training_dataset: name of the training dataset
        :training_dataset_version: version of the training dataset

    Returns:
        The training dataset if it finds it, otherwise exception

    Raises:
        :TrainingDatasetNotFound: if the requested training dataset could not be found
    """
    try:
        return training_datasets[fs_utils._get_table_name(training_dataset, training_dataset_version)]
    except KeyError:
        training_dataset_names = list(map(lambda td: fs_utils._get_table_name(td.name, td.version),
                                          training_datasets.values()))
        raise TrainingDatasetNotFound("Could not find the requested training dataset with name: {} " \
                                      "and version: {} among the list of available training datasets: {}".format(
            training_dataset,
            training_dataset_version,
            training_dataset_names))



def _find_featuregroup(featuregroups, featuregroup_name, featuregroup_version):
    """
    A helper function to look for a feature group name and version in a list of feature groups

    Args:
        :featuregroups: a list of featuregroup metadata in the feature store
        :featuregroup_name: name of the feature group
        :featuregroup_version: version of the feature group

    Returns:
        The feature group if it finds it, otherwise exception

    Raises:
        :FeaturegroupNotFound: if the requested feature group could not be found
    """
    try:
        return featuregroups[fs_utils._get_table_name(featuregroup_name, featuregroup_version)]
    except KeyError:
        featuregroup_names = list(map(lambda fg: fs_utils._get_table_name(fg.name, fg.version),
                                      featuregroups.values()))
        raise FeaturegroupNotFound("Could not find the requested feature group with name: {} " \
                                      "and version: {} among the list of available feature groups: {}".format(
            featuregroup_name,
            featuregroup_version,
            featuregroup_names))


def _get_feature_featuregroup_mapping(logical_query_plan, featurestore, featurestore_metadata):
    """
    Extracts a mapping of feature to featuregroup from the logical query plan.

    Args:
        :logical_query_plan: the logical query plan containing information about the features to be fetched
        :featurestore: the featurestore
        :featurestore_metadata: featurestore metadata

    Returns:
        a map of feature name to featuregroup
    """
    mapping = {}
    if isinstance(logical_query_plan.query, FeatureQuery):
        fg = logical_query_plan.featuregroups[0]
        mapping[_get_feature_short_name(logical_query_plan.query.feature)] = fs_utils._get_table_name(fg.name, fg.version)
    if isinstance(logical_query_plan.query, FeaturesQuery):
        for f in logical_query_plan.query.features:
            fg = _find_feature(f, featurestore, logical_query_plan.featuregroups)
            mapping[_get_feature_short_name(f)] = fs_utils._get_table_name(fg.name, fg.version)
    if isinstance(logical_query_plan.query, FeaturegroupQuery):
        fg = _find_featuregroup(featurestore_metadata.featuregroups, logical_query_plan.query.featuregroup,
                                logical_query_plan.query.featuregroup_version)
        for f in fg.features:
            mapping[_get_feature_short_name(f.name)] = fs_utils._get_table_name(fg.name, fg.version)
    return mapping


def _get_feature_short_name(feature_name):
    """
    Convert feature name to short name. If the feature name is on the form: fg.feature then it will return just the
    feature suffix. If the name does not include the featuregroup, this method does nothing.

    Args:
        :feature_name: name of the feature to get the short name of

    Returns:
        the short name of the feature
    """
    if constants.DELIMITERS.DOT_DELIMITER in feature_name:
        return feature_name.split(constants.DELIMITERS.DOT_DELIMITER)[1]
    return feature_name

