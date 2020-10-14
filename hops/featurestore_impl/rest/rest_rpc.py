"""
REST calls to Hopsworks Feature Store Service
"""

from hops import constants, util, hdfs
from hops.exceptions import RestAPIError
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

    if response.status_code != 200:
        response_object = response.json()
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not clear featuregroup contents (url: {}), server response: \n "
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))


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
    return util.http(constants.DELIMITERS.SLASH_DELIMITER +
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
                              features_schema, feature_corr_enabled,
                              featuregroup_desc_stats_enabled, features_histogram_enabled,
                              stat_columns, featuregroup_type, sql_query, jdbc_connector_id, online_fg):
    """
    Sends a REST call to hopsworks to create a new featuregroup with specified metadata

    Args:
        :featuregroup: the name of the featuregroup
        :featurestore_id: id of the featurestore where the featuregroup resides
        :description:  a description of the featuregroup
        :featuregroup_version: the version of the featuregroup (defaults to 1)
        :jobs: list of Hopsworks Jobs linked to the feature group
        :features_schema: the schema of the featuregroup
        :feature_corr_enabled: boolean to save feature correlation setting of the featuregroup
        :featuregroup_desc_stats_enabled: boolean to save descriptive statistics setting of the featuregroup
        :features_histogram_enabled: boolean to save features histogram setting of the featuregroup
        :stat_columns: a list of columns to compute statistics for
        :featuregroup_type: type of the featuregroup (on-demand or cached)
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
                     constants.REST_CONFIG.JSON_FEATUREGROUP_TYPE: featuregroup_type,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_ONLINE: online_fg,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_DESCRIPTIVE_STATISTICS_ENABLED: featuregroup_desc_stats_enabled,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURE_CORRELATION_ENABLED: feature_corr_enabled,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURE_HISTOGRAM_ENABLED: features_histogram_enabled,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_STATISTIC_COLUMNS: stat_columns
                     }
    if featuregroup_type == "onDemandFeaturegroupDTO":
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


def _enable_featuregroup_online_rest(featuregroup, featuregroup_version, featuregroup_id, featurestore_id,
                                     featuregroup_type, features_schema):
    """
    Makes a REST call to hopsworks appservice for enabling online serving of a feature group (Create MySQL Table)

    Args:
        :featuregroup_id: id of the featuregroup
        :featurestore_id: id of the featurestore where the featuregroup resides
        :featuregroup_type: type of the featuregroup (on-demand or cached)
        :features_schema: the schema of the featuregroup (to create the MySQL table)
        :featuregroup: name of the featuregroup
        :featuregroup_version: version of the featuregroup

    Returns:
        The REST response

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    json_contents = {constants.REST_CONFIG.JSON_TYPE: featuregroup_type,
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
                                      featuregroup_type):
    """
    Makes a REST call to hopsworks appservice for disable online serving of a feature group (Drop MySQL table)

    Args:
        :featuregroup_name: name of the featuregroup
        :featuregroup_version: version
        :featuregroup_id: id of the featuregroup
        :featurestore_id: id of the featurestore where the featuregroup resides
        :featuregroup_type: type of the featuregroup (on-demand or cached)

    Returns:
        The REST response

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    json_contents = {constants.REST_CONFIG.JSON_TYPE: featuregroup_type,
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
                                training_dataset_type, settings,
                                  connector_id = None, path = None):
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
        :training_dataset_type: type of the training dataset (external or hopsfs)
        :hopsfs_connector_id: id of the connector for a hopsfs training dataset
        :s3_connector_id: id of the connector for a s3 training dataset
        :settings: featurestore settings
        :path: the path within the storage connector where to save the training dataset

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
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_FORMAT: data_format,
                     constants.REST_CONFIG.JSON_FEATURESTORE_SETTINGS_TRAINING_DATASET_TYPE: training_dataset_type,
                     constants.REST_CONFIG.JSON_FEATURESTORE_LOCATION: path,
                     constants.REST_CONFIG.JSON_TRAINING_DATASET_CONNECTOR_ID: connector_id 
                     }

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
                                            featuregroup_type):
    """
    Sends a REST call to hopsworks to synchronize a Hive table with the feature store

    Args:
        :featuregroup: the name of the featuregroup
        :featurestore_id: id of the featurestore where the featuregroup resides
        :description:  a description of the featuregroup
        :featuregroup_version: the version of the featuregroup (defaults to 1)
        :jobs: list of Hopsworks Jobs linked to the feature group
        :featuregroup_type: type of the featuregroup (on-demand or cached)

    Returns:
        The HTTP response

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    json_contents = {constants.REST_CONFIG.JSON_FEATUREGROUP_NAME: featuregroup,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION: featuregroup_version,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_DESCRIPTION: description,
                     constants.REST_CONFIG.JSON_FEATUREGROUP_JOBS: _pre_process_jobs_list(jobs),
                     constants.REST_CONFIG.JSON_TYPE: featuregroup_type,
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


def _add_tag(featurestore_id, id, tag, value, resource):
    """
    Makes a REST call to Hopsworks to attach tags to a featuregroup or training dataset

    Args:
        :featurestore_id: the id of the featurestore
        :id: the id of the featuregroup or training dataset
        :tag: name of the tag to attach
        :value: value of the tag
        :resource: featuregroup or training dataset resource

    Returns:
        None

    """
    method = constants.HTTP_CONFIG.HTTP_PUT
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    str(featurestore_id) + constants.DELIMITERS.SLASH_DELIMITER +
                    resource +
                    constants.DELIMITERS.SLASH_DELIMITER + str(id) + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORE_TAGS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    tag)
    if value is not None:
        resource_url = resource_url + "?value=" + value

    response = util.send_request(method, resource_url)
    response_object = response.json()
    if response.status_code >= 400:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not attach tags (url: {}), server response: \n " \
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))


def _get_tags(featurestore_id, id, resource):
    """
    Makes a REST call to Hopsworks to get tags attached to a featuregroup or training dataset

    Args:
        :featurestore_id: the id of the featurestore
        :id: the id of the featuregroup or training dataset
        :resource: featuregroup or training dataset resource

    Returns:
        A dictionary containing the tags

    """
    method = constants.HTTP_CONFIG.HTTP_GET
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    str(featurestore_id) + constants.DELIMITERS.SLASH_DELIMITER +
                    resource +
                    constants.DELIMITERS.SLASH_DELIMITER + str(id) + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORE_TAGS_RESOURCE)
    response = util.send_request(method, resource_url, headers=headers)
    response_object = response.json()
    if response.status_code >= 400:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not get tags (url: {}), server response: \n " \
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))

    tags = {}
    if "items" in response_object:
        for tag in response_object["items"]:
            tags[tag["name"]] = tag["value"]

    return tags


def _remove_tag(featurestore_id, id, tag, resource):
    """
    Makes a REST call to Hopsworks to delete tags attached to a featuregroup or training dataset

    Args:
        :featurestore_id: the id of the featurestore
        :id: the id of the featuregroup or training dataset
        :tag: name of the tag
        :resource: featuregroup or training dataset resource

    Returns:
        None

    """
    method = constants.HTTP_CONFIG.HTTP_DELETE
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    str(featurestore_id) + constants.DELIMITERS.SLASH_DELIMITER +
                    resource +
                    constants.DELIMITERS.SLASH_DELIMITER + str(id) + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORE_TAGS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    tag)
    response = util.send_request(method, resource_url)
    if response.status_code >= 400:
        response_object = response.json()
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not remove tags (url: {}), server response: \n " \
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))


def _get_fs_tags():
    """
    Makes a REST call to Hopsworks to get tags that can be attached to featuregroups or training datasets

    Returns:
        List of tags

    """
    method = constants.HTTP_CONFIG.HTTP_GET
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE +
                    constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATURESTORE_TAGS_RESOURCE)
    response = util.send_request(method, resource_url, headers=headers)
    response_object = response.json()
    if response.status_code >= 400:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not get tags (url: {}), server response: \n " \
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))

    results = []
    if 'items' in response_object:
        for tag in response_object['items']:
            results.append(tag['name'])
    return results