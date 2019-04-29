"""
REST calls to Hopsworks Feature Store Service
"""

from hops import constants, util, hdfs
from hops.featurestore_impl.exceptions.exceptions import RestAPIError
import json


def _delete_table_contents(featuregroup_id, featurestore_id):
    """
    Sends a request to clear the contents of a featuregroup by dropping the featuregroup and recreating it with
    the same metadata.

    Args:
        :featuregroup_id: id of the featuregroup
        :featurestore_id: id of the featurestore

    Returns:
        The JSON response

    """
    method = constants.HTTP_CONFIG.HTTP_POST
    connection = util._get_http_connection(https=True)
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    str(featurestore_id) + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATUREGROUPS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    str(featuregroup_id) + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATUREGROUP_CLEAR_RESOURCE)
    response = util.send_request(connection, method, resource_url)
    resp_body = response.read()
    response_object = json.loads(resp_body)
    try:  # for python 3
        if response.code != 200:
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise RestAPIError("Could not clear featuregroup contents (url: {}), server response: \n "
                                 "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                resource_url, response.code, response.reason, error_code, error_msg, user_msg))
    except:  # for python 2
        if response.status != 200:
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise RestAPIError("Could not clear featuregroup contents (url: {}), server response: \n " \
                                 "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                resource_url, response.code, response.reason, error_code, error_msg, user_msg))
    return response_object


def _get_featurestores():
    """
    Sends a REST request to get all featurestores for the project

    Returns:
        a list of Featurestore JSON DTOs
    """
    method = constants.HTTP_CONFIG.HTTP_GET
    connection = util._get_http_connection(https=True)
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE)
    response = util.send_request(connection, method, resource_url)
    resp_body = response.read()
    response_object = json.loads(resp_body)
    try:  # for python 3
        if response.code != 200:
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise RestAPIError("Could not fetch feature stores (url: {}), server response: \n " \
                                 "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                resource_url, response.code, response.reason, error_code, error_msg, user_msg))
    except:  # for python 2
        if response.status != 200:
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise RestAPIError("Could not fetch feature stores (url: {}), server response: \n " \
                                 "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                resource_url, response.code, response.reason, error_code, error_msg, user_msg))
    return response_object


def _get_featurestore_metadata(featurestore):
    """
    Makes a REST call to the appservice in hopsworks to get all metadata of a featurestore (featuregroups and
    training datasets) for the provided featurestore.

    Args:
        :featurestore: the name of the database, defaults to the project's featurestore
    Returns:
        JSON response
    """
    method = constants.HTTP_CONFIG.HTTP_GET
    connection = util._get_http_connection(https=True)
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    featurestore + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORE_METADATA_RESOURCE)
    response = util.send_request(connection, method, resource_url)
    resp_body = response.read()
    response_object = json.loads(resp_body)
    try:  # for python 3
        if response.code != 200:
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise RestAPIError("Could not fetch featurestore metadata for featurestore: {} (url: {}), "
                                 "server response: \n "
                                 "HTTP code: {}, HTTP reason: {}, error code: {}, "
                                 "error msg: {}, user msg: {}".format(
                resource_url, featurestore, response.code, response.reason, error_code, error_msg, user_msg))
    except:  # for python 2
        if response.status != 200:
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise RestAPIError("Could not fetch featurestore metadata for featurestore: {} (url: {}), "
                                 "server response: \n " \
                                 "HTTP code: {}, HTTP reason: {}, error code: {}, "
                                 "error msg: {}, user msg: {}".format(
                resource_url, featurestore, response.code, response.reason, error_code, error_msg, user_msg))
    return response_object


def _create_featuregroup_rest(featuregroup, featurestore_id, description, featuregroup_version, job_name, dependencies,
                              features_schema, feature_corr_data, featuregroup_desc_stats_data,
                              features_histogram_data, cluster_analysis_data):
    """
    Sends a REST call to hopsworks to create a new featuregroup with specified metadata

    Args:
        :featuregroup: the name of the featuregroup
        :featurestore_id: id of the featurestore where the featuregroup resides
        :description:  a description of the featuregroup
        :featuregroup_version: the version of the featuregroup (defaults to 1)
        :job_name: the name of the job to compute the featuregroup
        :dependencies: list of the datasets that this featuregroup depends on (e.g input datasets to the feature
                       engineering job)
        :features_schema: the schema of the featuregroup
        :feature_corr_data: json-string with the feature correlation matrix of the featuregroup
        :featuregroup_desc_stats_data: json-string with the descriptive statistics of the featurergroup
        :features_histogram_data: list of json-strings with histogram data for the features in the featuregroup
        :cluster_analysis_data: cluster analysis for the featuregroup

    Returns:
        The HTTP response

    """
    json_contents = {constants.REST_CONFIG.JSON_FEATUREGROUP_NAME: featuregroup,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION: featuregroup_version,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_DESCRIPTION: description,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_JOBNAME: job_name,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_DEPENDENCIES: dependencies,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES: features_schema,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURE_CORRELATION: feature_corr_data,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_DESC_STATS: featuregroup_desc_stats_data,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES_HISTOGRAM: features_histogram_data,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES_CLUSTERS: cluster_analysis_data,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_UPDATE_METADATA: False,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_UPDATE_STATS: False}
    json_embeddable = json.dumps(json_contents)
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method = constants.HTTP_CONFIG.HTTP_POST
    connection = util._get_http_connection(https=True)
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    str(featurestore_id) + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATUREGROUPS_RESOURCE)
    response = util.send_request(connection, method, resource_url, body=json_embeddable, headers=headers)
    resp_body = response.read()
    response_object = json.loads(resp_body)
    try:  # for python 3
        if response.code != 201 and response.code != 200:
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise RestAPIError("Could not create feature group (url: {}), server response: \n " \
                                 "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                resource_url, response.code, response.reason, error_code, error_msg, user_msg))
    except:  # for python 2
        if response.status != 201 and response.status != 200:
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise RestAPIError("Could not create feature group (url: {}), server response: \n " \
                                 "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                resource_url, response.status, response.reason, error_code, error_msg, user_msg))
    return response_object


def _update_featuregroup_stats_rest(featuregroup_id, featurestore_id, featuregroup,
                                    featuregroup_version, feature_corr,
                                    featuregroup_desc_stats_data, features_histogram_data, cluster_analysis_data):
    """
    Makes a REST call to hopsworks appservice for updating the statistics of a particular featuregroup

    Args:
        :featuregroup_id: id of the featuregroup
        :featurestore_id: id of the featurestore where the featuregroup resides
        :featuregroup: the featuregroup to update statistics for
        :featuregroup_version: the version of the featuregroup
        :feature_corr: the feature correlation matrix
        :featuregroup_desc_stats_data: the descriptive statistics of the featuregroup
        :features_histogram_data: the histograms of the features in the featuregroup
        :cluster_analysis_data: the clusters from cluster analysis on the featuregroup

    Returns:
        The REST response
    """
    json_contents = {constants.REST_CONFIG.JSON_FEATUREGROUP_NAME: featuregroup,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION: featuregroup_version,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_JOBNAME: None,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_DEPENDENCIES: [],
                     constants.REST_CONFIG.JSON_FEATUREGROUP_UPDATE_METADATA: False,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_UPDATE_STATS: True,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURE_CORRELATION: feature_corr,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_DESC_STATS: featuregroup_desc_stats_data,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES_HISTOGRAM: features_histogram_data,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES_CLUSTERS: cluster_analysis_data,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES: []}
    json_embeddable = json.dumps(json_contents, allow_nan=False)
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method = constants.HTTP_CONFIG.HTTP_PUT
    connection = util._get_http_connection(https=True)
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    str(featurestore_id) +
                    constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATUREGROUPS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER
                    + str(featuregroup_id))
    response = util.send_request(connection, method, resource_url, body=json_embeddable, headers=headers)
    resp_body = response.read()
    response_object = json.loads(resp_body)
    try:  # for python 3
        if (response.code != 200):
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise RestAPIError("Could not update featuregroup stats (url: {}), server response: \n " \
                                 "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                resource_url, response.code, response.reason, error_code, error_msg, user_msg))
    except:  # for python 2
        if (response.status != 200):
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise RestAPIError("Could not update featuregroup stats (url: {}), server response: \n " \
                                 "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                resource_url, response.status, response.reason, error_code, error_msg, user_msg))
    return response_object


def _create_training_dataset_rest(training_dataset, featurestore_id, description, training_dataset_version,
                                  data_format, job_name, dependencies, features_schema_data,
                                  feature_corr_data, training_dataset_desc_stats_data, features_histogram_data,
                                  cluster_analysis_data):
    """
    Makes a REST request to hopsworks for creating a new training dataset

    Args:
        :training_dataset: the name of the training dataset
        :featurestore_id: the id of the featurestore where the training dataset resides
        :description: a description of the training dataset
        :training_dataset_version: the version of the training dataset (defaults to 1)
        :data_format: the format of the training dataset
        :job_name: the name of the job to compute the training dataset
        :dependencies: list of the datasets that this training dataset depends on (e.g input datasets to
                       the feature engineering job)
        :features_schema_data: the schema of the training dataset
        :feature_corr_data: json-string with the feature correlation matrix of the training dataset
        :cluster_analysis_data: the clusters from cluster analysis on the dataset
        :training_dataset_desc_stats_data: json-string with the descriptive statistics of the training dataset
        :features_histogram_data: list of json-strings with histogram data for the features in the training dataset

    Returns:
        the HTTP response

    """
    json_contents = {constants.REST_CONFIG.JSON_TRAINING_DATASET_NAME: training_dataset,
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_VERSION: training_dataset_version,
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_DESCRIPTION: description,
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_JOBNAME: job_name,
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_DEPENDENCIES: dependencies,
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_SCHEMA: features_schema_data,
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_FEATURE_CORRELATION: feature_corr_data,
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_DESC_STATS: training_dataset_desc_stats_data,
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_FEATURES_HISTOGRAM: features_histogram_data,
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_CLUSTERS: cluster_analysis_data,
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_FORMAT: data_format}
    json_embeddable = json.dumps(json_contents)
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method = constants.HTTP_CONFIG.HTTP_POST
    connection = util._get_http_connection(https=True)
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    str(featurestore_id) + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_TRAININGDATASETS_RESOURCE)
    response = util.send_request(connection, method, resource_url, body=json_embeddable, headers=headers)
    resp_body = response.read()
    response_object = json.loads(resp_body)
    try:  # for python 3
        if response.code != 201 and response.code != 200:
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise RestAPIError("Could not create training dataset (url: {}), server response: \n " \
                                 "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                resource_url, response.code, response.reason, error_code, error_msg, user_msg))
    except:  # for python 2
        if response.status != 201 and response.status != 200:
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise RestAPIError("Could not create training dataset (url: {}), server response: \n " \
                                 "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                resource_url, response.status, response.reason, error_code, error_msg, user_msg))
    return response_object


def _update_training_dataset_stats_rest(
        training_dataset, training_dataset_id, featurestore_id, training_dataset_version, features_schema,
        feature_corr_data, featuregroup_desc_stats_data, features_histogram_data, cluster_analysis_data):
    """
    A helper function that makes a REST call to hopsworks for updating the stats and schema metadata about a
    training dataset

    Args:
        :training_dataset: the name of the training dataset
        :training_dataset_id: id of the training dataset
        :featurestore_id: id of the featurestore that the training dataset is linked to
        :training_dataset_version: the version of the training dataset (defaults to 1)
        :features_schema: the schema of the training dataset
        :feature_corr_data:  json-string with the feature correlation matrix of the training dataset
        :featuregroup_desc_stats_data: json-string with the descriptive statistics of the training dataset
        :features_histogram_data: list of json-strings with histogram data for the features in the training dataset
        :cluster_analysis_data: the clusters from cluster analysis on the dataset

    Returns:
        the HTTP response

    """
    json_contents = {constants.REST_CONFIG.JSON_TRAINING_DATASET_NAME: training_dataset,
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_VERSION: training_dataset_version,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURE_CORRELATION: feature_corr_data,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_DESC_STATS: featuregroup_desc_stats_data,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES_HISTOGRAM: features_histogram_data,
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_CLUSTERS: cluster_analysis_data,
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_SCHEMA: features_schema,
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_DEPENDENCIES: [],
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_UPDATE_METADATA: False,
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_UPDATE_STATS: True}
    json_embeddable = json.dumps(json_contents)
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method = constants.HTTP_CONFIG.HTTP_PUT
    connection = util._get_http_connection(https=True)
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    str(featurestore_id) + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_TRAININGDATASETS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER
                    + str(training_dataset_id))
    response = util.send_request(connection, method, resource_url, body=json_embeddable, headers=headers)
    resp_body = response.read()
    response_object = json.loads(resp_body)
    try:  # for python 3
        if (response.code != 200):
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise RestAPIError("Could not update training dataset stats (url: {}), server response: \n " \
                                 "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                resource_url, response.code, response.reason, error_code, error_msg, user_msg))
    except:  # for python 2
        if (response.status != 200):
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise RestAPIError("Could not update training dataset stats (url: {}), server response: \n " \
                                 "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                resource_url, response.status, response.reason, error_code, error_msg, user_msg))
    return response_object