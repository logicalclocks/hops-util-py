"""
AWS temporary credential provider.

"""

from hops import constants, util, hdfs
from hops.exceptions import RestAPIError
import os


def assume_role(role_arn=None, role_session_name=None, duration_seconds=3600):
    """
    Assume a role and sets the temporary credential to the spark context hadoop configuration and environment variables.

    Args:
        :role_arn: (string) the role arn to be assumed
        :role_session_name: (string) use to uniquely identify a session when the same role is assumed by different principals or for different reasons.
        :duration_seconds: (int) the duration of the session. Maximum session duration is 3600 seconds.

    >>> from hops.credentials_provider import assume_role
    >>> assume_role(role_arn="arn:aws:iam::<AccountNumber>:role/analyst")

    or

    >>> assume_role() # to assume the default role

    Returns:
        temporary credentials
    """
    query = _query_string(role_arn, role_session_name, duration_seconds)
    method = constants.HTTP_CONFIG.HTTP_GET
    resource_url = _get_cloud_resource() + constants.REST_CONFIG.HOPSWORKS_AWS_CLOUD_SESSION_TOKEN_RESOURCE + query

    response = util.send_request(method, resource_url)
    json_content = _parse_response(response, resource_url)
    _set_spark_hadoop_conf(json_content)
    _set_envs(json_content)
    return json_content


def get_roles():
    """
    Get all roles mapped to the current project

    >>> from hops.credentials_provider import get_roles
    >>> get_roles()

    Returns:
        A list of role arn
    """
    json_content = _get_roles()
    items = json_content[constants.REST_CONFIG.JSON_ARRAY_ITEMS]
    roles = []
    for role in items:
        roles.append(role[constants.REST_CONFIG.JSON_CLOUD_ROLE])
    return roles


def get_role(role_id="default"):
    """
    Get a role arn mapped to the current project by id or if no id is supplied the default role is returned

    Args:
        :role_id: id of the role default

    >>> from hops.credentials_provider import get_role
    >>> get_role(id)
    or
    >>> get_role() # to get the default role

    Returns:
        A role arn
    """
    json_content = _get_roles(role_id=role_id)
    return json_content[constants.REST_CONFIG.JSON_CLOUD_ROLE]


def _set_spark_hadoop_conf(json_content):
    spark = None
    if constants.ENV_VARIABLES.SPARK_IS_DRIVER in os.environ:
        spark = util._find_spark()
    if spark is not None:
        sc = spark.sparkContext
        sc._jsc.hadoopConfiguration().set(constants.S3_CONFIG.S3_CREDENTIAL_PROVIDER_ENV,
                                          constants.S3_CONFIG.S3_TEMPORARY_CREDENTIAL_PROVIDER)
        sc._jsc.hadoopConfiguration().set(constants.S3_CONFIG.S3_ACCESS_KEY_ENV,
                                          json_content[constants.REST_CONFIG.JSON_ACCESS_KEY_ID])
        sc._jsc.hadoopConfiguration().set(constants.S3_CONFIG.S3_SECRET_KEY_ENV,
                                          json_content[constants.REST_CONFIG.JSON_SECRET_KEY_ID])
        sc._jsc.hadoopConfiguration().set(constants.S3_CONFIG.S3_SESSION_KEY_ENV,
                                          json_content[constants.REST_CONFIG.JSON_SESSION_TOKEN_ID])


def _set_envs(json_content):
    os.environ[constants.S3_CONFIG.AWS_ACCESS_KEY_ID_ENV] = json_content[constants.REST_CONFIG.JSON_ACCESS_KEY_ID]
    os.environ[constants.S3_CONFIG.AWS_SECRET_ACCESS_KEY_ENV] = json_content[constants.REST_CONFIG.JSON_SECRET_KEY_ID]
    os.environ[constants.S3_CONFIG.AWS_SESSION_TOKEN_ENV] = json_content[constants.REST_CONFIG.JSON_SESSION_TOKEN_ID]


def _get_roles(role_id=None):
    by_id = ""
    if role_id:
        by_id = constants.DELIMITERS.SLASH_DELIMITER + str(role_id)
    method = constants.HTTP_CONFIG.HTTP_GET
    resource_url = _get_cloud_resource() + constants.REST_CONFIG.HOPSWORKS_CLOUD_ROLE_MAPPINGS_RESOURCE + by_id
    response = util.send_request(method, resource_url)
    return _parse_response(response, resource_url)


def _parse_response(response, url):
    if response.ok:
        return response.json()
    else:
        error_code, error_msg, user_msg = util._parse_rest_error(response.json())
        raise RestAPIError("Error calling {}. Got status: HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}"
                           ", user msg: {}".format(url, response.status_code, response.reason, error_code, error_msg,
                                                   user_msg))


def _query_string(role, role_session_name, duration_seconds):
    query = ""
    if role:
        query = constants.REST_CONFIG.HOPSWORKS_CLOUD_SESSION_TOKEN_RESOURCE_QUERY_ROLE + "=" + role
    if role_session_name:
        if query != "":
            query += "&"
        query += constants.REST_CONFIG.HOPSWORKS_CLOUD_SESSION_TOKEN_RESOURCE_QUERY_SESSION + "=" + role_session_name
    if duration_seconds != 3600 and duration_seconds > 0:
        if query != "":
            query += "&"
        query += constants.REST_CONFIG.HOPSWORKS_CLOUD_SESSION_TOKEN_RESOURCE_QUERY_SESSION_DURATION + "=" + \
                 str(duration_seconds)

    if query != "":
        query = "?" + query
    return query


def _get_cloud_resource():
    return constants.DELIMITERS.SLASH_DELIMITER + constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + \
        constants.DELIMITERS.SLASH_DELIMITER + constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + \
        constants.DELIMITERS.SLASH_DELIMITER + hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER + \
        constants.REST_CONFIG.HOPSWORKS_CLOUD_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER
