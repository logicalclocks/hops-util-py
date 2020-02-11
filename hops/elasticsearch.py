"""
A module for setting up Elastcisearch spark connector.
"""

from hops import constants, util, hdfs, tls
from hops.exceptions import RestAPIError
import os


def _get_elasticsearch_url():
    return os.environ[constants.ENV_VARIABLES.ELASTIC_ENDPOINT_ENV_VAR]


def get_elasticsearch_index(index):
    """
    Get the valid elasticsearch index for later use. This helper method prefix the index name with the project name.

    Args:
        :index: the elasticsearch index to interact with.

    Returns:
        A valid elasticsearch index name.
    """
    return hdfs.project_name() + "_" + index


def get_elasticsearch_config(index):
    """
    Get the required elasticsearch configuration to setup a connection using spark connector.

    Args:
        :index: the elasticsearch index to interact with.

    Returns:
        A dictionary with required configuration.
    """
    config = {
        constants.ELASTICSEARCH_CONFIG.SSL_CONFIG: "true",
        constants.ELASTICSEARCH_CONFIG.NODES: _get_elasticsearch_url(),
        constants.ELASTICSEARCH_CONFIG.NODES_WAN_ONLY: "true",
        constants.ELASTICSEARCH_CONFIG.SSL_KEYSTORE_LOCATION: tls.get_key_store(),
        constants.ELASTICSEARCH_CONFIG.SSL_KEYSTORE_PASSWORD: tls.get_key_store_pwd(),
        constants.ELASTICSEARCH_CONFIG.SSL_TRUSTSTORE_LOCATION: tls.get_trust_store(),
        constants.ELASTICSEARCH_CONFIG.SSL_TRUSTSTORE_PASSWORD: tls.get_trust_store_pwd(),
        constants.ELASTICSEARCH_CONFIG.HTTP_AUTHORIZATION: get_authorization_token(),
        constants.ELASTICSEARCH_CONFIG.INDEX: get_elasticsearch_index(index)
    }
    return config


def get_authorization_token():
    """
    Get the authorization token to interact with elasticsearch.

    Args:

    Returns:
        The authorization token to be used in http header.
    """
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method = constants.HTTP_CONFIG.HTTP_GET
    resource_url = constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_ELASTIC_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_ELASTIC_JWT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   hdfs.project_id()
    response = util.send_request(method, resource_url, headers=headers)
    response_object = response.json()
    if response.status_code >= 400:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not get authorization token for elastic (url: {}), server response: \n " \
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))

    return "Bearer " + response_object["token"]
