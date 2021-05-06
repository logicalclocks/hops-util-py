"""

Utility functions to manage secrets in Hopsworks.

"""

from hops import constants, util, project
from hops.exceptions import RestAPIError
import json

def create_secret(name, secret, project_name=None):
    """
    Create a secret

    Creating a secret for this user

    >>> from hops import secret
    >>> secret_token = 'DIOK4jmgFdwadjnDDW98'
    >>> secret.create_secret('my_secret', secret_token)

    Creating a secret and share it with all members of a project

    >>> from hops import secret
    >>> secret_token = 'DIOK4jmgFdwadjnDDW98'
    >>> secret.create_secret('my_secret', secret_token, project_name='someproject')

    Args:
        name: Name of the secret to create
        secret: Value of the secret
        project_name: Name of the project to share the secret with
    """

    secret_config = {'name': name, 'secret': secret}

    if project_name is None:
      secret_config['visibility'] = "PRIVATE"
    else:
      scope_project = project.get_project_info(project_name)
      secret_config['scope'] = scope_project['projectId']
      secret_config['visibility'] = "PROJECT"

    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method = constants.HTTP_CONFIG.HTTP_POST
    resource_url = constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_USER_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_SECRETS_RESOURCE
    response = util.send_request(method, resource_url, data=json.dumps(secret_config), headers=headers)
    response_object = response.json()
    if response.status_code >= 400:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not create secret (url: {}), server response: \n "
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))

def get_secret(name, owner=None):
    """
    Get a secret

    Get a secret for this user

    >>> from hops import secret
    >>> secret.get_secret('my_secret')

    Get a secret shared with this project by the secret owner

    >>> from hops import secret
    >>> secret.get_secret('shared_secret', owner='username')
    Args:
        name: Name of the secret to get
        owner: The username of the user that created the secret
    Returns:
        The secret
    """
    if owner is None:
      resource_url = constants.DELIMITERS.SLASH_DELIMITER + \
                     constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                     constants.REST_CONFIG.HOPSWORKS_USER_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                     constants.REST_CONFIG.HOPSWORKS_SECRETS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                     name
    else:
      resource_url = constants.DELIMITERS.SLASH_DELIMITER + \
                     constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                     constants.REST_CONFIG.HOPSWORKS_USER_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                     constants.REST_CONFIG.HOPSWORKS_SECRETS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                     constants.REST_CONFIG.HOPSWORKS_SHARED + "?name=" + name + "&owner=" + owner

    method = constants.HTTP_CONFIG.HTTP_GET

    response = util.send_request(method, resource_url)
    response_object = response.json()
    if response.status_code >= 400:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not get secret (url: {}), server response: \n "
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))

    return response_object['items'][0]['secret']

def delete_secret(name):
    """

    Delete a secret for this user

    >>> from hops import secret
    >>> secret.delete_secret('my_secret')

    Args:
        name: Name of the secret to delete
    """
    resource_url = constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_USER_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_SECRETS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   name

    method = constants.HTTP_CONFIG.HTTP_DELETE

    response = util.send_request(method, resource_url)
    if response.status_code >= 400:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not delete secret (url: {}), server response: \n "
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))