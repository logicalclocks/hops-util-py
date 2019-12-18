"""

Utility functions to manage jobs in Hopsworks.

"""
from hops import constants, util, hdfs
from hops.exceptions import RestAPIError
import json


def create_job(name, job_config):
    """
    Create a job in Hopsworks

    Args:
        name: Name of the job to be created.
        job_config: A dictionary representing the job configuration

    Returns:
        HTTP(S)Connection
    """
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    job_config["appName"] = name
    method = constants.HTTP_CONFIG.HTTP_PUT
    resource_url = constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_JOBS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   name
    response = util.send_request(method, resource_url, data=json.dumps(job_config), headers=headers)
    response_object = response.json()
    if response.status_code >= 400:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not create job (url: {}), server response: \n "
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))

    return response_object


def _job_execution_action(name, args=None):
    """
    Manages execution for the given job, start or stop. Submits an http request to the HOPSWORKS REST API.

    Returns:
        The job status.
    """
    method = constants.HTTP_CONFIG.HTTP_PUT
    resource_url = constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_JOBS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   name + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_EXECUTIONS_RESOURCE

    response = util.send_request(method, resource_url, args)
    response_object = response.json()
    if response.status_code >= 400:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not perform action on job's execution (url: {}), server response: \n "
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))

    return response_object


def start_job(name, args=None):
    """
    Start an execution of the job. Only one execution can be active for a job.

    Returns:
        The job status.
    """
    method = constants.HTTP_CONFIG.HTTP_POST
    resource_url = constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_JOBS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   name + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_EXECUTIONS_RESOURCE

    response = util.send_request(method, resource_url, args)
    response_object = response.json()
    if response.status_code >= 400:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not perform action on job's execution (url: {}), server response: \n "
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))

    return response_object


def stop_job(name):
    """
    Stop the current execution of the job.
    Returns:
        The job status.
    """
    method = constants.HTTP_CONFIG.HTTP_PUT
    resource_url = constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_JOBS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   name + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_EXECUTIONS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   "status"

    status = {"status":"stopped"}
    response = util.send_request(method, resource_url, data=status)
    response_object = response.json()
    if response.status_code >= 400:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not perform action on job's execution (url: {}), server response: \n "
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))

    return response_object


def get_executions(name, query=None):
    """
    Get a list of the currently running executions for this job.
    Returns:
        The job status.
    """
    method = constants.HTTP_CONFIG.HTTP_GET
    resource_url = constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_JOBS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   name + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_EXECUTIONS_RESOURCE + query
    response = util.send_request(method, resource_url)
    response_object = response.json()
    if response.status_code >= 500:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not get current job's execution (url: {}), server response: \n "
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))
    if response.status_code >= 400:
        return None
    return response_object
