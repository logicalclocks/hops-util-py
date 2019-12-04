"""
API for attaching, detaching, and reading extended metadata to HopsFS files/directories.
It uses the Hopsworks /xattrs REST API
"""
from hops import constants, util, hdfs
from hops.exceptions import RestAPIError
import urllib


def set_xattr(hdfs_path, xattr_name, value):
    """
    Attach an extended attribute to an hdfs_path

    Args:
        :hdfs_path: path of a file or directory
        :xattr_name: name of the extended attribute
        :value: value of the extended attribute

    Returns:
        None
    """
    value = str(value)
    hdfs_path = urllib.parse.quote(hdfs._expand_path(hdfs_path))
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method = constants.HTTP_CONFIG.HTTP_PUT
    resource_url = constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_XATTR_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   hdfs_path + constants.DELIMITERS.QUESTION_MARK_DELIMITER + constants.XATTRS.XATTRS_PARAM_NAME + \
                   constants.DELIMITERS.JDBC_CONNECTION_STRING_VALUE_DELIMITER + xattr_name
    response = util.send_request(method, resource_url, data=value, headers=headers)
    response_object = response.json()
    if response.status_code >= 400:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not attach extened attributes from a path (url: {}), server response: \n " \
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))


def get_xattr(hdfs_path, xattr_name=None):
    """
    Get the extended attribute attached to an hdfs_path.

    Args:
        :hdfs_path: path of a file or directory
        :xattr_name: name of the extended attribute

    Returns:
        A dictionary with the extended attribute(s) as key value pair(s). If the :xattr_name is None,
         the API returns all associated extended attributes.
    """
    hdfs_path = urllib.parse.quote(hdfs._expand_path(hdfs_path))
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method = constants.HTTP_CONFIG.HTTP_GET
    resource_url = constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_XATTR_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   hdfs_path
    if xattr_name is not None:
        resource_url += constants.DELIMITERS.QUESTION_MARK_DELIMITER + constants.XATTRS.XATTRS_PARAM_NAME + \
                        constants.DELIMITERS.JDBC_CONNECTION_STRING_VALUE_DELIMITER + xattr_name

    response = util.send_request(method, resource_url, headers=headers)
    response_object = response.json()
    if response.status_code >= 400:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not get extened attributes attached to a path (url: {}), server response: \n " \
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))

    results = {}
    for item in response_object["items"]:
        results[item["name"]] = item["value"]
    return results


def remove_xattr(hdfs_path, xattr_name):
    """
    Remove an extended attribute attached to an hdfs_path

    Args:
        :hdfs_path: path of a file or directory
        :xattr_name: name of the extended attribute

    Returns:
        None
    """
    hdfs_path = urllib.parse.quote(hdfs._expand_path(hdfs_path))
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method = constants.HTTP_CONFIG.HTTP_DELETE
    resource_url = constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_XATTR_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   hdfs_path + constants.DELIMITERS.QUESTION_MARK_DELIMITER + constants.XATTRS.XATTRS_PARAM_NAME + \
                   constants.DELIMITERS.JDBC_CONNECTION_STRING_VALUE_DELIMITER + xattr_name
    response = util.send_request(method, resource_url, headers=headers)
    if response.status_code >= 400:
        response_object = response.json()
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not remove extened attributes from a path (url: {}), server response: \n " \
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))