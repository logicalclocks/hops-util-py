"""
A module for connecting to and working with Hopsworks projects.

Using the utility functions you can connect to a project of a particular Hopsworks instance which sets up all the
required environment variables and configuration parameters. Then you can use moduels such as `dataset` to interact
with particular services of a project.
"""

import os
import json
from hops import util, constants


def connect(project, host=None, port=443, scheme="https", hostname_verification=False,
            api_key=None,
            region_name=constants.AWS.DEFAULT_REGION,
            secrets_store=constants.LOCAL.LOCAL_STORE,
            trust_store_path=None):
    """
    Connect to a project of a Hopworks instance. Sets up API key and REST API endpoint.

    Example usage:

    >>> project.connect("dev_featurestore", "localhost", api_key="api_key_file")

    Args:
        :project_name: the name of the project to be used
        :host: the hostname of the Hopsworks cluster. If none specified, the library will attempt to the one set by the environment variable constants.ENV_VARIABLES.REST_ENDPOINT_END_VAR
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

    util.connect(host, port, scheme, hostname_verification, api_key, region_name, secrets_store, trust_store_path)
    os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_NAME_ENV_VAR] = project

    # Get projectId as we need for the REST endpoint
    project_info = get_project_info(project)
    project_id = str(project_info['projectId'])
    os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_ID_ENV_VAR] = project_id


def create(new_project, owner=None):
    """
    Creates a project in Hopsworks.

    >>> from hops import util, project
    >>> new_project = {"projectName": "MyProject4", "description": "", "retentionPeriod": "", "status": 0,
    >>>                "services": ["JOBS", "KAFKA", "JUPYTER", "HIVE", "SERVING", "FEATURESTORE", "AIRFLOW"]}
    >>>
    >>> util.connect("localhost", api_key="api_key_file")
    >>> project.create(new_project)

    Args:
        :new_project: A dictionary with the new project attributes.
        :owner: Create a project for another user (owner). Only admin user can use this option.

    Returns:
        JSON response

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    if owner is None:
        project_endpoint = constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                           "?projectName=" + new_project['projectName']
    else:
        project_endpoint = constants.REST_CONFIG.HOPSWORKS_ADMIN_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                           constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + "s" + \
                           constants.DELIMITERS.SLASH_DELIMITER + "createas"
        new_project["owner"] = owner

    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    return util.http(constants.DELIMITERS.SLASH_DELIMITER +
                     constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                     project_endpoint,
                     headers=headers,
                     method=constants.HTTP_CONFIG.HTTP_POST,
                     data=json.dumps(new_project))


def get_project_info(project_name):
    """
    Makes a REST call to hopsworks to get all metadata of a project for the provided project.

    Args:
        :project_name: the name of the project

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
                     project_name)

def get_project_info_as_shared(project_name):
    """
    Makes a REST call to hopsworks to get all metadata of a project for the provided project.

    Args:
        :project_name: the name of the project

    Returns:
        JSON response
        See https://github.com/logicalclocks/hopsworks-ee/blob/master/hopsworks-common/src/main/java/io/hops/hopsworks/common/project/ProjectDTO.java

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    return util.http(constants.DELIMITERS.SLASH_DELIMITER +
                     constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                     constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                     constants.REST_CONFIG.HOPSWORKS_AS_SHARED + constants.DELIMITERS.SLASH_DELIMITER +
                     constants.REST_CONFIG.HOPSWORKS_PROJECT_INFO_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                     project_name)

def project_id_as_shared(name=None):
    """
    Get the Hopsworks project id from the project name. This endpoint can be used also for projects parents of shared datasets

    Args:
         :name: the name of the project, current project if none is supplied
    Returns: the Hopsworks project id

    """
    if not name:
        return os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_ID_ENV_VAR]

    project_info = get_project_info_as_shared(name)
    return str(project_info['projectId'])