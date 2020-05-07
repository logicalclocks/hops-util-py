"""
A module for connecting to and working with Hopsworks projects.

Using the utility functions you can connect to a project of a particular Hopsworks instance which sets up all the
required environment variables and configuration parameters. Then you can use moduels such as `dataset` to interact
with particular services of a project.
"""

import os
import warnings

from hops import util, constants
from hops.exceptions import APIKeyFileNotFound


def connect(project, host=None, port=443, scheme="https", hostname_verification=False,
            api_key=None,
            region_name=constants.AWS.DEFAULT_REGION,
            secrets_store=constants.LOCAL.LOCAL_STORE,
            trust_store_path=None):
    """
    Connect to a project of a Hopworks instance. Sets up API key and REST API endpoint.

    Example usage:

    >>> project.connect("dev_featurestore", "hops.site", api_key="api_key_file")

    Args:
        :project: the name of the project to be used
        :host: the hostname of the Hopsworks cluster. If none specified, the library will attempt to the one set by
        the environment variable constants.ENV_VARIABLES.REST_ENDPOINT_END_VAR
        :port: the REST port of the Hopsworks cluster
        :scheme: the scheme to use for connection to the REST API.
        :hostname_verification: whether or not to verify Hopsworks' certificate - default True
        :api_key: path to a file containing an API key or the actual API key value. For secrets_store=local only.
        :region_name: The name of the AWS region in which the required secrets are stored
        :secrets_store: The secrets storage to be used. Secretsmanager or parameterstore for AWS, local otherwise.
        :trust_store_path: path to the file  containing the Hopsworks certificates

    Returns:
        None
    """

    if host is not None:
        os.environ[constants.ENV_VARIABLES.REST_ENDPOINT_END_VAR] = scheme + "://" + host + ":" + str(port)

    os.environ[constants.ENV_VARIABLES.REGION_NAME_ENV_VAR] = region_name
    if secrets_store == constants.LOCAL.LOCAL_STORE and not api_key:
        warnings.warn("API key was not provided and secrets_store is local. "
                      "When the connect method is used outside of a Hopsworks instance, it is recommended to "
                      "use an API key. Falling back to JWT...")
    else:
        try:
            os.environ[constants.ENV_VARIABLES.API_KEY_ENV_VAR] = util.get_secret(secrets_store, 'api-key', api_key)
        except APIKeyFileNotFound:
            warnings.warn("API key file was not found. Will use the provided api_key value as the API key")

    if trust_store_path is not None:
        os.environ[constants.ENV_VARIABLES.DOMAIN_CA_TRUSTSTORE_PEM_ENV_VAR] = trust_store_path

    os.environ[constants.ENV_VARIABLES.REQUESTS_VERIFY_ENV_VAR] = str(hostname_verification).lower()
    os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_NAME_ENV_VAR] = project

    # Get projectId as we need for the REST endpoint
    project_info = get_project_info(project)
    project_id = str(project_info['projectId'])
    os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_ID_ENV_VAR] = project_id


def get_project_info(project):
    """
    Makes a REST call to hopsworks to get all metadata of a project for the provided project.

    Args:
        :project: the name of the project

    Returns:
        JSON response
        See https://github.com/logicalclocks/hopsworks-ee/blob/master/hopsworks-common/src/main/java/io/hops/hopsworks/common/project/ProjectDTO.java

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    
    return util.http(constants.DELIMITERS.SLASH_DELIMITER +
                     constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                     constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                     constants.REST_CONFIG.HOPSWORKS_PROJECT_INFO_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                     project)
