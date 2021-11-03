"""

Utility functions to manage users in Hopsworks.

"""
import json
from hops import constants, util

import logging
log = logging.getLogger(__name__)

def create_user(new_user):
    """
    Create a user in Hopsworks. Registers and activates a user with role HOPS_USER.

    Example usage:

    >>> from hops import util, user
    >>> new_user = {"firstName":"Joe","lastName":"Doe","email":"joe@hopsworks.ai","telephoneNum":"",
    >>>             "chosenPassword":"Admin123","repeatedPassword":"Admin123",
    >>>             "securityQuestion":"What is your oldest sibling's middle name?","securityAnswer":"Admin123",
    >>>             "tos":"true","authType":"Mobile","twoFactor":"false","toursEnabled":"true","orgName":"","dep":"",
    >>>             "street":"","city":"","postCode":"","country":"","testUser":"false"}
    >>> util.connect("localhost", api_key="api_key_file")
    >>> user.create(new_user)

    Args:
        :new_user: Dict with the new user attributes

    Returns:
        None
    """
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    # Register user
    util.http(constants.DELIMITERS.SLASH_DELIMITER + \
              constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
              constants.REST_CONFIG.HOPSWORKS_AUTH_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
              constants.REST_CONFIG.HOPSWORKS_AUTH_RESOURCE_REGISTER,
              headers=headers,
              method=constants.HTTP_CONFIG.HTTP_POST,
              data=json.dumps(new_user))

    # Get user id
    response = util.http(constants.DELIMITERS.SLASH_DELIMITER + \
                         constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                         constants.REST_CONFIG.HOPSWORKS_ADMIN_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                         constants.REST_CONFIG.HOPSWORKS_USERS_RESOURCE + "?filter_by=user_email:" + new_user["email"],
                         headers=headers,
                         method=constants.HTTP_CONFIG.HTTP_GET)
    user_profile = response['items'][0]
    user_profile["status"] = "VERIFIED_ACCOUNT"
    # Verify user
    util.http(constants.DELIMITERS.SLASH_DELIMITER + \
              constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
              constants.REST_CONFIG.HOPSWORKS_ADMIN_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
              constants.REST_CONFIG.HOPSWORKS_USERS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
              str(user_profile['id']) + constants.DELIMITERS.SLASH_DELIMITER,
              headers=headers,
              method=constants.HTTP_CONFIG.HTTP_PUT,
              data=json.dumps(user_profile))
    # Accept user
    response = util.http(constants.DELIMITERS.SLASH_DELIMITER + \
                         constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                         constants.REST_CONFIG.HOPSWORKS_ADMIN_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                         constants.REST_CONFIG.HOPSWORKS_USERS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                         str(user_profile['id']) + constants.DELIMITERS.SLASH_DELIMITER + "accepted",
                         headers=headers,
                         method=constants.HTTP_CONFIG.HTTP_PUT,
                         data=json.dumps(user_profile))
    log.info(response)
