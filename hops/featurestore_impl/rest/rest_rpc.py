"""
REST calls to Hopsworks Feature Store Service
"""

from hops import constants, util, hdfs
from hops.exceptions import RestAPIError
import json
from json.decoder import JSONDecodeError

def _http(resource_url, headers=None, method=constants.HTTP_CONFIG.HTTP_GET, data=None):
    response = util.send_request(method, resource_url, headers=headers, data=data)
    try:
        response_object = response.json()
    except JSONDecodeError:
        response_object = None

    if (response.status_code // 100) != 2:
        if response_object:
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        else:
            error_code, error_msg, user_msg = "", "", ""

        raise RestAPIError("Could not execute HTTP request (url: {}), server response: \n "
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))

    return response_object

def _delete_table_contents(featuregroup_id, featurestore_id):
    """
    Sends a request to clear the contents of a featuregroup by dropping the featuregroup and recreating it with
    the same metadata.

    Args:
        :featuregroup_id: id of the featuregroup
        :featurestore_id: id of the featurestore

    Returns:
        The JSON response

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    method = constants.HTTP_CONFIG.HTTP_POST
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    str(featurestore_id) + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATUREGROUPS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    str(featuregroup_id) + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATUREGROUP_CLEAR_RESOURCE)
    response = util.send_request(method, resource_url)
    response_object = response.json()

    if response.status_code != 200:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not clear featuregroup contents (url: {}), server response: \n "
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))

    return response_object


def _get_featurestores():
    """
    Sends a REST request to get all featurestores for the project

    Returns:
        a list of Featurestore JSON DTOs

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    method = constants.HTTP_CONFIG.HTTP_GET
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE)
    response = util.send_request(method, resource_url)
    response_object = response.json()

    if response.status_code != 200:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not fetch feature stores (url: {}), server response: \n " \
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))

    return response_object


def _get_featurestore_metadata(featurestore):
    """
    Makes a REST call to hopsworks to get all metadata of a featurestore (featuregroups and
    training datasets) for the provided featurestore.

    Args:
        :featurestore: the name of the database, defaults to the project's featurestore

    Returns:
        JSON response

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    method = constants.HTTP_CONFIG.HTTP_GET
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    featurestore + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORE_METADATA_RESOURCE)
    response = util.send_request(method, resource_url)
    response_object = response.json()

    if response.status_code != 200:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not fetch featurestore metadata for featurestore: {} (url: {}), "
                           "server response: \n "
                           "HTTP code: {}, HTTP reason: {}, error code: {}, "
                           "error msg: {}, user msg: {}".format(
            resource_url, featurestore, response.status_code, response.reason, error_code, error_msg, user_msg))

    return response_object

def _get_project_info(project_name):
    """
    Makes a REST call to hopsworks to get all metadata of a project for the provided project.

    Args:
        :project_name: the name of the project

    Returns:
        JSON response

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    return _http(constants.DELIMITERS.SLASH_DELIMITER +
                 constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                 constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                 constants.REST_CONFIG.HOPSWORKS_PROJECT_INFO_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                 project_name)

def _get_credentials(project_id):
    """
    Makes a REST call to hopsworks for getting the project user certificates needed to connect to services such as Hive

    Args:
        :project_name: id of the project

    Returns:
        JSON response

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    return _http(constants.DELIMITERS.SLASH_DELIMITER +
                 constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                 constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                 project_id + constants.DELIMITERS.SLASH_DELIMITER +
                 constants.REST_CONFIG.HOPSWORKS_PROJECT_CREDENTIALS_RESOURCE)

def _get_client(project_id):
    """
    """    

    resource_url= (constants.DELIMITERS.SLASH_DELIMITER +
                 constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                 constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                 project_id + constants.DELIMITERS.SLASH_DELIMITER +
                 constants.REST_CONFIG.HOPSWORKS_PROJECT_CLIENT)
    response = util.send_request(constants.HTTP_CONFIG.HTTP_GET, resource_url, stream=True)
    if (response.status_code // 100) != 2:
        error_code, error_msg, user_msg = "", "", ""

        raise RestAPIError("Could not execute HTTP request (url: {}), server response: \n "
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))

    return response

def _pre_process_jobs_list(jobNames):
    """
    Convert list of jobNames to list of JobDTOs that is expected by the backend

    Args:
        :jobNames: list of job names

    Returns:
        list of job dtos
    """
    jobs_dtos = []
    for jobName in jobNames:
        jobs_dtos.append({
            constants.REST_CONFIG.JSON_FEATURESTORE_JOB_NAME: jobName,
            constants.REST_CONFIG.JSON_FEATURESTORE_JOB_ID: None,
            constants.REST_CONFIG.JSON_FEATURESTORE_JOB_LAST_COMPUTED: None,
            constants.REST_CONFIG.JSON_FEATURESTORE_JOB_STATUS: None,
            constants.REST_CONFIG.JSON_FEATURESTORE_ID: None,
            constants.REST_CONFIG.JSON_FEATURESTORE_JOB_FEATUREGROUP_ID: None,
            constants.REST_CONFIG.JSON_FEATURESTORE_JOB_TRAINING_DATASET_ID: None,
        })
    return jobs_dtos


def _create_featuregroup_rest(featuregroup, featurestore_id, description, featuregroup_version, jobs,
                              features_schema, feature_corr_data, featuregroup_desc_stats_data,
                              features_histogram_data, cluster_analysis_data, feature_corr_enabled,
                              featuregroup_desc_stats_enabled, features_histogram_enabled,
                              cluster_analysis_enabled, stat_columns, num_bins, num_clusters, corr_method,
                              featuregroup_type, featuregroup_dto_type, sql_query, jdbc_connector_id, online_fg):
    """
    Sends a REST call to hopsworks to create a new featuregroup with specified metadata

    Args:
        :featuregroup: the name of the featuregroup
        :featurestore_id: id of the featurestore where the featuregroup resides
        :description:  a description of the featuregroup
        :featuregroup_version: the version of the featuregroup (defaults to 1)
        :jobs: list of Hopsworks Jobs linked to the feature group
        :features_schema: the schema of the featuregroup
        :feature_corr_data: json-string with the feature correlation matrix of the featuregroup
        :featuregroup_desc_stats_data: json-string with the descriptive statistics of the featurergroup
        :features_histogram_data: list of json-strings with histogram data for the features in the featuregroup
        :cluster_analysis_data: cluster analysis for the featuregroup
        :feature_corr_enabled: boolean to save feature correlation setting of the featuregroup
        :featuregroup_desc_stats_enabled: boolean to save descriptive statistics setting of the featuregroup
        :features_histogram_enabled: boolean to save features histogram setting of the featuregroup
        :cluster_analysis_enabled: boolean to save cluster analysis setting of the featuregroup
        :stat_columns: a list of columns to compute statistics for
        :num_bins: number of bins to use for computing histograms
        :num_clusters: the number of clusters to use for cluster analysis
        :corr_method: the method to compute feature correlation with (pearson or spearman)
        :featuregroup_type: type of the featuregroup (on-demand or cached)
        :featuregroup_dto_type: type of the JSON DTO for the backend
        :sql_query: SQL Query for On-demand feature groups
        :jdbc_connector_id: id of the jdbc_connector for on-demand feature groups
        :online_fg: whether online feature serving should be enabled for the feature group

    Returns:
        The HTTP response

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    json_contents = {constants.REST_CONFIG.JSON_FEATUREGROUP_NAME: featuregroup,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION: featuregroup_version,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_DESCRIPTION: description,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_JOBS: _pre_process_jobs_list(jobs),
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES: features_schema,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURE_CORRELATION: feature_corr_data,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_DESC_STATS: featuregroup_desc_stats_data,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES_HISTOGRAM: features_histogram_data,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES_CLUSTERS: cluster_analysis_data,
                     constants.REST_CONFIG.JSON_TYPE: featuregroup_dto_type,
                     constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_FEATUREGROUP_TYPE: featuregroup_type,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_ONLINE: online_fg,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_CLUSTER_ANALYSIS_ENABLED: cluster_analysis_enabled,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_DESCRIPTIVE_STATISTICS_ENABLED: featuregroup_desc_stats_enabled,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURE_CORRELATION_ENABLED: feature_corr_enabled,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURE_HISTOGRAM_ENABLED: features_histogram_enabled,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_STATISTIC_COLUMNS: stat_columns,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_NUM_BINS: num_bins,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_NUM_CLUSTERS: num_clusters,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_CORR_METHOD: corr_method
                     }
    if featuregroup_type == constants.REST_CONFIG.JSON_FEATUREGROUP_ON_DEMAND_TYPE:
        json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_ON_DEMAND_QUERY] = sql_query
        json_contents[constants.REST_CONFIG.JSON_FEATUREGROUP_JDBC_CONNECTOR_ID] = jdbc_connector_id
    json_embeddable = json.dumps(json_contents, allow_nan=False)
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method = constants.HTTP_CONFIG.HTTP_POST
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    str(featurestore_id) + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATUREGROUPS_RESOURCE)
    response = util.send_request(method, resource_url, data=json_embeddable, headers=headers)
    response_object = response.json()

    if response.status_code != 201 and response.status_code != 200:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not create feature group (url: {}), server response: \n " \
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))

    return response_object


def _update_featuregroup_stats_rest(featuregroup_id, featurestore_id, feature_corr,
                                    featuregroup_desc_stats_data, features_histogram_data, cluster_analysis_data,
                                    descriptive_statistics, feature_correlation, feature_histograms, cluster_analysis,
                                    stat_columns, num_bins, num_clusters, corr_method, featuregroup_type,
                                    featuregroup_dto_type, jobs):
    """
    Makes a REST call to hopsworks appservice for updating the statistics of a particular featuregroup

    Args:
        :featuregroup_id: id of the featuregroup
        :featurestore_id: id of the featurestore where the featuregroup resides
        :feature_corr: the feature correlation matrix
        :featuregroup_desc_stats_data: the descriptive statistics of the featuregroup
        :features_histogram_data: the histograms of the features in the featuregroup
        :cluster_analysis_data: the clusters from cluster analysis on the featuregroup
        :descriptive_statistics: the setting to be saved for descripte statistics of the featuregroup
        :feature_correlation: the setting to be saved for feature correlation of the featuregroup
        :feature_histograms: the setting to be saved for feature histograms of the featuregroup
        :cluster_analysis: the setting to be saved for cluster analysis of the featuregroup
        :stat_columns: the columns to compute statistics of the featuregroup for
        :num_bins: the number of bins to use for the histograms of the featuregroup
        :num_clusters: the number of clusters to use for the cluster analysis of the featuregroup
        :corr_method: the correlation method to use for the correlation analysis of the featuregroup
        :featuregroup_type: type of the featuregroup (on-demand or cached)
        :featuregroup_dto_type: type of the JSON DTO for the backend
        :jobs: a list of jobs for updating the feature group stats

    Returns:
        The REST response

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    json_contents = {constants.REST_CONFIG.JSON_FEATUREGROUP_JOBS: _pre_process_jobs_list(jobs),
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURE_CORRELATION: feature_corr,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_DESC_STATS: featuregroup_desc_stats_data,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES_HISTOGRAM: features_histogram_data,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES_CLUSTERS: cluster_analysis_data,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURE_CORRELATION_ENABLED: feature_correlation,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_DESCRIPTIVE_STATISTICS_ENABLED: descriptive_statistics,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURE_HISTOGRAM_ENABLED: feature_histograms,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_CLUSTER_ANALYSIS_ENABLED: cluster_analysis,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_STATISTIC_COLUMNS: stat_columns,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_NUM_BINS: num_bins,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_NUM_CLUSTERS: num_clusters,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_CORR_METHOD: corr_method,
                     constants.REST_CONFIG.JSON_TYPE: featuregroup_dto_type,
                     constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_FEATUREGROUP_TYPE: featuregroup_type,
                     }
    json_embeddable = json.dumps(json_contents, allow_nan=False)
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method = constants.HTTP_CONFIG.HTTP_PUT
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    str(featurestore_id) +
                    constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATUREGROUPS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER
                    + str(featuregroup_id) + "?" + constants.REST_CONFIG.JSON_FEATURESTORE_UPDATE_STATS_QUERY_PARAM
                    + "=true" + constants.DELIMITERS.AMPERSAND_DELIMITER +
                    constants.REST_CONFIG.JSON_FEATURESTORE_UPDATE_METADATA_QUERY_PARAM + "=false"
                    + constants.DELIMITERS.AMPERSAND_DELIMITER +
                    constants.REST_CONFIG.JSON_FEATURESTORE_ENABLE_ONLINE_QUERY_PARAM
                    + "=false" + constants.DELIMITERS.AMPERSAND_DELIMITER +
                    constants.REST_CONFIG.JSON_FEATURESTORE_DISABLE_ONLINE_QUERY_PARAM + "=false"
                    + constants.DELIMITERS.AMPERSAND_DELIMITER +
                    constants.REST_CONFIG.JSON_FEATURESTORE_UPDATE_STATISTICS_SETTINGS + "=true"
                    + constants.DELIMITERS.AMPERSAND_DELIMITER +
                    constants.REST_CONFIG.JSON_FEATURESTORE_UPDATE_JOB_QUERY_PARAM + "=" + json.dumps(len(jobs) > 0)
                    )
    response = util.send_request(method, resource_url, data=json_embeddable, headers=headers)
    response_object = response.json()
    if response.status_code != 200:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not update featuregroup stats (url: {}), server response: \n " \
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))

    return response_object


def _enable_featuregroup_online_rest(featuregroup, featuregroup_version, featuregroup_id, featurestore_id,
                                     featuregroup_dto_type, featuregroup_type, features_schema):
    """
    Makes a REST call to hopsworks appservice for enabling online serving of a feature group (Create MySQL Table)

    Args:
        :featuregroup_id: id of the featuregroup
        :featurestore_id: id of the featurestore where the featuregroup resides
        :featuregroup_type: type of the featuregroup (on-demand or cached)
        :featuregroup_dto_type: type of the JSON DTO for the backend
        :features_schema: the schema of the featuregroup (to create the MySQL table)
        :featuregroup: name of the featuregroup
        :featuregroup_version: version of the featuregroup

    Returns:
        The REST response

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    json_contents = {constants.REST_CONFIG.JSON_TYPE: featuregroup_dto_type,
                     constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_FEATUREGROUP_TYPE: featuregroup_type,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES: features_schema,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_NAME: featuregroup,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION: featuregroup_version
                     }
    json_embeddable = json.dumps(json_contents, allow_nan=False)
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method = constants.HTTP_CONFIG.HTTP_PUT
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    str(featurestore_id) +
                    constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATUREGROUPS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER
                    + str(featuregroup_id) + "?" + constants.REST_CONFIG.JSON_FEATURESTORE_ENABLE_ONLINE_QUERY_PARAM
                    + "=true" + constants.DELIMITERS.AMPERSAND_DELIMITER +
                    constants.REST_CONFIG.JSON_FEATURESTORE_UPDATE_METADATA_QUERY_PARAM + "=false"
                    + constants.DELIMITERS.AMPERSAND_DELIMITER
                    + constants.REST_CONFIG.JSON_FEATURESTORE_UPDATE_STATS_QUERY_PARAM
                    + "=false" + constants.DELIMITERS.AMPERSAND_DELIMITER +
                    constants.REST_CONFIG.JSON_FEATURESTORE_DISABLE_ONLINE_QUERY_PARAM + "=false"
                    )
    response = util.send_request(method, resource_url, data=json_embeddable, headers=headers)
    response_object = response.json()
    if response.status_code != 200:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not enable feature serving (url: {}), server response: \n " \
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))

    return response_object


def _disable_featuregroup_online_rest(featuregroup_name, featuregroup_version, featuregroup_id, featurestore_id,
                                      featuregroup_dto_type, featuregroup_type):
    """
    Makes a REST call to hopsworks appservice for disable online serving of a feature group (Drop MySQL table)

    Args:
        :featuregroup_name: name of the featuregroup
        :featuregroup_version: version
        :featuregroup_id: id of the featuregroup
        :featurestore_id: id of the featurestore where the featuregroup resides
        :featuregroup_type: type of the featuregroup (on-demand or cached)
        :featuregroup_dto_type: type of the JSON DTO for the backend

    Returns:
        The REST response

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    json_contents = {constants.REST_CONFIG.JSON_TYPE: featuregroup_dto_type,
                     constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_FEATUREGROUP_TYPE: featuregroup_type,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_NAME: featuregroup_name,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION: featuregroup_version
                     }
    json_embeddable = json.dumps(json_contents, allow_nan=False)
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method = constants.HTTP_CONFIG.HTTP_PUT
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    str(featurestore_id) +
                    constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATUREGROUPS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER
                    + str(featuregroup_id) + "?" + constants.REST_CONFIG.JSON_FEATURESTORE_DISABLE_ONLINE_QUERY_PARAM
                    + "=true" + constants.DELIMITERS.AMPERSAND_DELIMITER +
                    constants.REST_CONFIG.JSON_FEATURESTORE_UPDATE_METADATA_QUERY_PARAM + "=false"
                    + constants.DELIMITERS.AMPERSAND_DELIMITER
                    + constants.REST_CONFIG.JSON_FEATURESTORE_UPDATE_STATS_QUERY_PARAM
                    + "=false" + constants.DELIMITERS.AMPERSAND_DELIMITER +
                    constants.REST_CONFIG.JSON_FEATURESTORE_ENABLE_ONLINE_QUERY_PARAM + "=false"
                    )
    response = util.send_request(method, resource_url, data=json_embeddable, headers=headers)
    response_object = response.json()
    if response.status_code != 200:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not disable feature serving (url: {}), server response: \n " \
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))
    return response_object


def _create_training_dataset_rest(training_dataset, featurestore_id, description, training_dataset_version,
                                  data_format, jobs, features_schema_data,
                                  feature_corr_data, training_dataset_desc_stats_data, features_histogram_data,
                                  cluster_analysis_data, training_dataset_type, training_dataset_dto_type,
                                  settings, hopsfs_connector_id = None, s3_connector_id = None):
    """
    Makes a REST request to hopsworks for creating a new training dataset

    Args:
        :training_dataset: the name of the training dataset
        :featurestore_id: the id of the featurestore where the training dataset resides
        :description: a description of the training dataset
        :training_dataset_version: the version of the training dataset (defaults to 1)
        :data_format: the format of the training dataset
        :jobs: list of Hopsworks jobs linked to the training dataset
        :features_schema_data: the schema of the training dataset
        :feature_corr_data: json-string with the feature correlation matrix of the training dataset
        :cluster_analysis_data: the clusters from cluster analysis on the dataset
        :training_dataset_desc_stats_data: json-string with the descriptive statistics of the training dataset
        :features_histogram_data: list of json-strings with histogram data for the features in the training dataset
        :training_dataset_type: type of the training dataset (external or hopsfs)
        :training_dataset_dto_type: type of the JSON DTO for the backend
        :hopsfs_connector_id: id of the connector for a hopsfs training dataset
        :s3_connector_id: id of the connector for a s3 training dataset
        :settings: featurestore settings

    Returns:
        the HTTP response

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    json_contents = {constants.REST_CONFIG.JSON_TRAINING_DATASET_NAME: training_dataset,
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_VERSION: training_dataset_version,
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_DESCRIPTION: description,
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_JOBS: _pre_process_jobs_list(jobs),
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_SCHEMA: features_schema_data,
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_FEATURE_CORRELATION: feature_corr_data,
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_DESC_STATS: training_dataset_desc_stats_data,
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_FEATURES_HISTOGRAM: features_histogram_data,
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_CLUSTERS: cluster_analysis_data,
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_FORMAT: data_format,
                     constants.REST_CONFIG.JSON_TYPE: training_dataset_dto_type,
                     constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_TRAINING_DATASET_TYPE: training_dataset_type
                     }
    if training_dataset_type == settings.external_training_dataset_type:
        json_contents[constants.REST_CONFIG.JSON_TRAINING_DATASET_S3_CONNECTOR_ID] = s3_connector_id
    if training_dataset_type == settings.hopsfs_training_dataset_type:
        json_contents[constants.REST_CONFIG.JSON_TRAINING_DATASET_HOPSFS_CONNECTOR_ID] = hopsfs_connector_id
    json_embeddable = json.dumps(json_contents, allow_nan=False)
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method = constants.HTTP_CONFIG.HTTP_POST
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    str(featurestore_id) + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_TRAININGDATASETS_RESOURCE)
    response = util.send_request(method, resource_url, data=json_embeddable, headers=headers)
    response_object = response.json()

    if response.status_code != 201 and response.status_code != 200:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not create training dataset (url: {}), server response: \n " \
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))

    return response_object


def _update_training_dataset_stats_rest(
        training_dataset_id, featurestore_id, feature_corr_data, featuregroup_desc_stats_data,
        features_histogram_data, cluster_analysis_data, training_dataset_type, training_dataset_dto_type, jobs):
    """
    A helper function that makes a REST call to hopsworks for updating the stats and schema metadata about a
    training dataset

    Args:
        :training_dataset_id: id of the training dataset
        :featurestore_id: id of the featurestore that the training dataset is linked to
        :feature_corr_data:  json-string with the feature correlation matrix of the training dataset
        :featuregroup_desc_stats_data: json-string with the descriptive statistics of the training dataset
        :features_histogram_data: list of json-strings with histogram data for the features in the training dataset
        :cluster_analysis_data: the clusters from cluster analysis on the dataset
        :training_dataset_type: type of the training dataset (external or hopsfs)
        :training_dataset_dto_type: type of the JSON DTO for the backend
        :jobs: list of jobs for updating the training dataset stats

    Returns:
        the HTTP response

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    json_contents = {constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURE_CORRELATION: feature_corr_data,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_DESC_STATS: featuregroup_desc_stats_data,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES_HISTOGRAM: features_histogram_data,
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_CLUSTERS: cluster_analysis_data,
                     constants.REST_CONFIG.JSON_TYPE: training_dataset_dto_type,
                     constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_TRAINING_DATASET_TYPE: training_dataset_type,
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_JOBS: _pre_process_jobs_list(jobs)}
    json_embeddable = json.dumps(json_contents, allow_nan=False)
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method = constants.HTTP_CONFIG.HTTP_PUT
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    str(featurestore_id) + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_TRAININGDATASETS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER
                    + str(training_dataset_id) + "?" + constants.REST_CONFIG.JSON_FEATURESTORE_UPDATE_STATS_QUERY_PARAM
                    + "=true" + constants.DELIMITERS.AMPERSAND_DELIMITER +
                    constants.REST_CONFIG.JSON_FEATURESTORE_UPDATE_METADATA_QUERY_PARAM + "=false")
    response = util.send_request(method, resource_url, data=json_embeddable, headers=headers)
    response_object = response.json()

    if response.status_code != 200:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not update training dataset stats (url: {}), server response: \n " \
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))

    return response_object


def _get_featuregroup_rest(featuregroup_id, featurestore_id):
    """
    Makes a REST call to hopsworks for getting the metadata of a particular featuregroup (including the statistics)

    Args:
        :featuregroup_id: id of the featuregroup
        :featurestore_id: id of the featurestore where the featuregroup resides

    Returns:
        The REST response

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method = constants.HTTP_CONFIG.HTTP_GET
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    str(featurestore_id) +
                    constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATUREGROUPS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER
                    + str(featuregroup_id))
    response = util.send_request(method, resource_url, headers=headers)
    response_object = response.json()
    if response.status_code != 200:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not get the metadata of featuregroup (url: {}), server response: \n " \
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))

    return response_object


def _get_training_dataset_rest(training_dataset_id, featurestore_id):
    """
    Makes a REST call to hopsworks for getting the metadata of a particular training dataset (including the statistics)

    Args:
        :training_dataset_id: id of the training_dataset
        :featurestore_id: id of the featurestore where the training dataset resides

    Returns:
        The REST response

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method = constants.HTTP_CONFIG.HTTP_GET
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    str(featurestore_id) +
                    constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_TRAININGDATASETS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER
                    + str(training_dataset_id))
    response = util.send_request(method, resource_url, headers=headers)
    response_object = response.json()

    if response.status_code != 200:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not get the metadata of featuregroup (url: {}), server response: \n " \
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))

    return response_object


def _sync_hive_table_with_featurestore_rest(featuregroup, featurestore_id, description, featuregroup_version, jobs,
                                            feature_corr_data, featuregroup_desc_stats_data,
                                            features_histogram_data, cluster_analysis_data, featuregroup_type,
                                            featuregroup_dto_type):
    """
    Sends a REST call to hopsworks to synchronize a Hive table with the feature store

    Args:
        :featuregroup: the name of the featuregroup
        :featurestore_id: id of the featurestore where the featuregroup resides
        :description:  a description of the featuregroup
        :featuregroup_version: the version of the featuregroup (defaults to 1)
        :jobs: list of Hopsworks Jobs linked to the feature group
        :feature_corr_data: json-string with the feature correlation matrix of the featuregroup
        :featuregroup_desc_stats_data: json-string with the descriptive statistics of the featurergroup
        :features_histogram_data: list of json-strings with histogram data for the features in the featuregroup
        :cluster_analysis_data: cluster analysis for the featuregroup
        :featuregroup_type: type of the featuregroup (on-demand or cached)
        :featuregroup_dto_type: type of the JSON DTO for the backend

    Returns:
        The HTTP response

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    json_contents = {constants.REST_CONFIG.JSON_FEATUREGROUP_NAME: featuregroup,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION: featuregroup_version,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_DESCRIPTION: description,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_JOBS: _pre_process_jobs_list(jobs),
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURE_CORRELATION: feature_corr_data,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_DESC_STATS: featuregroup_desc_stats_data,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES_HISTOGRAM: features_histogram_data,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES_CLUSTERS: cluster_analysis_data,
                     constants.REST_CONFIG.JSON_TYPE: featuregroup_dto_type,
                     constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_FEATUREGROUP_TYPE: featuregroup_type
                     }

    json_embeddable = json.dumps(json_contents)
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method = constants.HTTP_CONFIG.HTTP_POST
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    str(featurestore_id) + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATUREGROUPS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATUREGROUPS_SYNC_RESOURCE)
    response = util.send_request(method, resource_url, data=json_embeddable, headers=headers)
    response_object = response.json()
    if response.status_code != 201 and response.status_code != 200:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not sync hive table with featurestore (url: {}), server response: \n " \
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))

    return response_object


def _get_online_featurestore_jdbc_connector_rest(featurestore_id):
    """
    Makes a REST call to Hopsworks to get the JDBC connection to the online feature store

    Args:
        :featurestore_id: the id of the featurestore

    Returns:
        the http response

    """
    method = constants.HTTP_CONFIG.HTTP_GET
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    str(featurestore_id) + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_STORAGE_CONNECTORS_RESOURCE +
                    constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_ONLINE_FEATURESTORE_STORAGE_CONNECTOR_RESOURCE)
    response = util.send_request(method, resource_url)
    response_object = response.json()
    if response.status_code != 200:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not fetch online featurestore connector (url: {}), server response: \n " \
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))
    return response_object


def _add_metadata(featurestore_id, featuregroup_id, name, value):
    """
    Makes a REST call to Hopsworks to attach extended metadata to a featuregroup

    Args:
        :featurestore_id: the id of the featurestore
        :featuregroup_id: the id of the featuregroup
        :name: the name of the extended attribute
        :value: the value of the extended attribute

    Returns:
        None

    """
    method = constants.HTTP_CONFIG.HTTP_PUT
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    data = json.dumps({name: str(value)})
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    str(featurestore_id) + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATUREGROUPS_RESOURCE +
                    constants.DELIMITERS.SLASH_DELIMITER + str(featuregroup_id) + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATUREGROUPS_XATTRS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    name)
    response = util.send_request(method, resource_url, data=data, headers=headers)
    response_object = response.json()
    if response.status_code >= 400:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not attach extened attributes to a featuregroup (url: {}), server response: \n " \
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))


def _get_metadata(featurestore_id, featuregroup_id, name=None):
    """
    Makes a REST call to Hopsworks to get extended metadata attached to a featuregroup

    Args:
        :featurestore_id: the id of the featurestore
        :featuregroup_id: the id of the featuregroup
        :name: the name of the extended attribute

    Returns:
        A dictionary containing the extended metadata

    """
    method = constants.HTTP_CONFIG.HTTP_GET
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    str(featurestore_id) + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATUREGROUPS_RESOURCE +
                    constants.DELIMITERS.SLASH_DELIMITER + str(featuregroup_id) + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATUREGROUPS_XATTRS_RESOURCE)
    if name is not None:
        resource_url += constants.DELIMITERS.SLASH_DELIMITER + name
    response = util.send_request(method, resource_url, headers=headers)
    response_object = response.json()
    if response.status_code >= 400:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not get extened attributes for a featuregroup (url: {}), server response: \n " \
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))

    results = {}
    for item in response_object["items"]:
        results[item["name"]] = item["value"]
    return results


def _remove_metadata(featurestore_id, featuregroup_id, name):
    """
    Makes a REST call to Hopsworks to delete extended metadata attached to a featuregroup

    Args:
        :featurestore_id: the id of the featurestore
        :featuregroup_id: the id of the featuregroup
        :name: the name of the extended attribute

    Returns:
        None

    """
    method = constants.HTTP_CONFIG.HTTP_DELETE
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    str(featurestore_id) + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATUREGROUPS_RESOURCE +
                    constants.DELIMITERS.SLASH_DELIMITER + str(featuregroup_id) + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATUREGROUPS_XATTRS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    name)
    response = util.send_request(method, resource_url, headers=headers)
    if response.status_code == 404:
        raise RestAPIError("Could not remove extened attributes from a featuregroup (url: {}), server response: \n " \
                           "HTTP code: {}, HTTP reason: {}".format(resource_url, response.status_code, response.reason))
    if response.status_code >= 400:
        response_object = response.json()
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not remove extened attributes from a featuregroup (url: {}), server response: \n " \
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))
