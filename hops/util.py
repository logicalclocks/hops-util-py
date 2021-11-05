"""

Miscellaneous utility functions for user applications.

"""

import os
import signal
import urllib
from ctypes import cdll
import socket
import warnings

import json

import boto3

from hops import constants
from hops import tls

from cryptography import x509
from cryptography.x509.oid import NameOID
from hops.exceptions import UnkownSecretStorageError, APIKeyFileNotFound
import base64
from socket import socket
from json.decoder import JSONDecodeError
from hops.exceptions import RestAPIError
from hops.exceptions import APIKeyFileNotFound


verify = None
#! Needed for hops library backwards compatability
try:
    import requests
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.SecurityWarning)
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except:
    pass

# in case importing in %%local
try:
    from pyspark.sql import SparkSession
except:
    pass

session = requests.session()


def http(resource_url, headers=None, method=constants.HTTP_CONFIG.HTTP_GET, data=None):
    response = send_request(method, resource_url, headers=headers, data=data)
    try:
        response_object = response.json()
    except JSONDecodeError:
        response_object = None

    if (response.status_code // 100) != 2:
        if response_object:
            error_code, error_msg, user_msg = _parse_rest_error(response_object)
        else:
            error_code, error_msg, user_msg = "", "", ""

        raise RestAPIError("Could not execute HTTP request (url: {}), server response: \n "
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))

    return response_object


def _get_elastic_endpoint():
    """

    Returns:
        The endpoint for putting things into elastic search

    """
    elastic_endpoint = os.environ[constants.ENV_VARIABLES.ELASTIC_ENDPOINT_ENV_VAR]
    host, port = elastic_endpoint.split(':')
    return host + ':' + port

def _get_hopsworks_rest_endpoint():
    """

    Returns:
        The hopsworks REST endpoint for making requests to the REST API

    """
    return os.environ[constants.ENV_VARIABLES.REST_ENDPOINT_END_VAR]

def _get_host_port_pair():
    """
    Removes "http or https" from the rest endpoint and returns a list
    [endpoint, port], where endpoint is on the format /path.. without http://

    Returns:
        a list [endpoint, port]
    """
    endpoint = _get_hopsworks_rest_endpoint()
    if 'http' in endpoint:
        last_index = endpoint.rfind('/')
        endpoint = endpoint[last_index + 1:]
    host_port_pair = endpoint.split(':')
    return host_port_pair


def connect(host=None, port=443, scheme="https", hostname_verification=False,
            api_key_file=None,
            region_name=constants.AWS.DEFAULT_REGION,
            secrets_store=constants.LOCAL.LOCAL_STORE,
            trust_store_path=None):
    """
    Connect to a Hopworks instance. Sets the REST API endpoint and any necessary authentication parameters.

    Example usage:

    >>> from hops import util
    >>> util.connect("localhost", api_key_file="api_key_file")

    Args:
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
    if host is not None:
        os.environ[constants.ENV_VARIABLES.REST_ENDPOINT_END_VAR] = scheme + "://" + host + ":" + str(port)

    os.environ[constants.ENV_VARIABLES.REGION_NAME_ENV_VAR] = region_name
    if secrets_store == constants.LOCAL.LOCAL_STORE and not api_key_file:
        warnings.warn("API key was not provided and secrets_store is local. "
                      "When the connect method is used outside of a Hopsworks instance, it is recommended to "
                      "use an API key. Falling back to JWT...")
    else:
        try:
            os.environ[constants.ENV_VARIABLES.API_KEY_ENV_VAR] = get_secret(secrets_store, 'api-key', api_key_file)
            if constants.ENV_VARIABLES.SERVING_API_KEY_ENV_VAR not in os.environ:
                os.environ[constants.ENV_VARIABLES.SERVING_API_KEY_ENV_VAR] = os.environ[constants.ENV_VARIABLES.API_KEY_ENV_VAR]
        except APIKeyFileNotFound:
            warnings.warn("API key file was not found. Will use the provided api_key value as the API key")
            os.environ[constants.ENV_VARIABLES.API_KEY_ENV_VAR] = api_key_file

    if trust_store_path is not None:
        os.environ[constants.ENV_VARIABLES.DOMAIN_CA_TRUSTSTORE_PEM_ENV_VAR] = trust_store_path

    os.environ[constants.ENV_VARIABLES.REQUESTS_VERIFY_ENV_VAR] = str(hostname_verification).lower()


def set_auth_header(headers):
    """
    Set authorization header for HTTP requests to Hopsworks, depending if setup is remote or not.

    Args:
        http headers
    """
    if constants.HTTP_CONFIG.HTTP_AUTHORIZATION in headers:
        return  # auth header already set

    if constants.ENV_VARIABLES.API_KEY_ENV_VAR in os.environ:
        headers[constants.HTTP_CONFIG.HTTP_AUTHORIZATION] = "ApiKey " + \
            os.environ[constants.ENV_VARIABLES.API_KEY_ENV_VAR]
    else:
        headers[constants.HTTP_CONFIG.HTTP_AUTHORIZATION] = "Bearer " + get_jwt()


def get_requests_verify(hostname, port):
    """
    Returns:
        if env var HOPS_UTIL_VERIFY is not false
            if the env variable is set, then use the certificate, otherwise return true
        return false
    """
    if constants.ENV_VARIABLES.REQUESTS_VERIFY_ENV_VAR in os.environ and os.environ[
        constants.ENV_VARIABLES.REQUESTS_VERIFY_ENV_VAR] == 'true':

        try:
            if constants.ENV_VARIABLES.DOMAIN_CA_TRUSTSTORE_ENV_VAR in os.environ:
                # need to convert the jks to pem
                return tls.get_ca_chain_location()
            elif constants.ENV_VARIABLES.DOMAIN_CA_TRUSTSTORE_PEM_ENV_VAR in os.environ: 
                return os.environ[constants.ENV_VARIABLES.DOMAIN_CA_TRUSTSTORE_PEM_ENV_VAR]
            else:
                return True
        except x509.ExtensionNotFound:
            return True

    return False


def send_request(method, resource, data=None, headers=None, stream=False, files=None):
    """
    Sends a request to Hopsworks. In case of Unauthorized response, submit the request once more as jwt might not
    have been read properly from local container.
    Args:
        method: HTTP(S) method
        resource: Hopsworks resource
        data: HTTP(S) payload
        headers: HTTP(S) headers
        stream: set the stream for the session object
        files: dictionary of {filename: fileobject} files to multipart upload.
    Returns:
        HTTP(S) response
    """
    if headers is None:
        headers = {}
    global verify
    host, port = _get_host_port_pair()
    if verify is None:
        verify = get_requests_verify(host, port)
    set_auth_header(headers)
    url = _get_hopsworks_rest_endpoint() + resource
    req = requests.Request(method, url, data=data, headers=headers, files=files)
    prepped = session.prepare_request(req)

    response = session.send(prepped, verify=verify, stream=stream)

    if response.status_code == constants.HTTP_CONFIG.HTTP_UNAUTHORIZED:
        set_auth_header(headers)
        prepped = session.prepare_request(req)
        response = session.send(prepped, stream=stream)
    return response

def _parse_rest_error(response_dict):
    """
    Parses a JSON response from hopsworks after an unsuccessful request

    Args:
        response_dict: the JSON response represented as a dict

    Returns:
        error_code, error_msg, user_msg
    """
    error_code = -1
    error_msg = ""
    user_msg = ""
    if constants.REST_CONFIG.JSON_ERROR_CODE in response_dict:
        error_code = response_dict[constants.REST_CONFIG.JSON_ERROR_CODE]
    if constants.REST_CONFIG.JSON_ERROR_MSG in response_dict:
        error_msg = response_dict[constants.REST_CONFIG.JSON_ERROR_MSG]
    if constants.REST_CONFIG.JSON_USR_MSG in response_dict:
        user_msg = response_dict[constants.REST_CONFIG.JSON_USR_MSG]
    return error_code, error_msg, user_msg

def get_job_name():
    """
    If this method is called from inside a hopsworks job, it returns the name of the job.

    Returns:
        the name of the hopsworks job

    """
    if constants.ENV_VARIABLES.JOB_NAME_ENV_VAR in os.environ:
        return os.environ[constants.ENV_VARIABLES.JOB_NAME_ENV_VAR]

def get_jwt():
    """
    Retrieves jwt from local container.

    Returns:
        Content of jwt.token file in local container.
    """
    jwt_path = ""
    if constants.ENV_VARIABLES.SECRETS_DIR_ENV_VAR in os.environ:
        jwt_path = os.environ[constants.ENV_VARIABLES.SECRETS_DIR_ENV_VAR]

    jwt_path = os.path.join(jwt_path, constants.REST_CONFIG.JWT_TOKEN)

    with open(jwt_path, "r") as jwt:
        return jwt.read()

def parse_redhift_jdbc_url(url):
    """
    Parses a Redshift JDBC URL and extracts region_name, cluster_identifier, database and user.

    Args:
        :url: the JDBC URL

    Returns:
        region_name, cluster_identifier, database, user
    """

    jdbc_url = urllib.parse.urlparse(url)
    redshift_url = urllib.parse.urlparse(jdbc_url.path)
    if redshift_url.scheme != 'redshift':
        raise Exception('Trying to parse non-redshift url: ' + jdbc_url)
    cluster_identifier = redshift_url.netloc.split('.')[0]
    region_name = redshift_url.netloc.split('.')[2]
    database = redshift_url.path.split('/')[1]
    user = urllib.parse.parse_qs(jdbc_url.query)['user'][0]
    return region_name, cluster_identifier, database, user


def get_redshift_username_password(region_name, cluster_identifier, user, database):
    """
    Requests temporary Redshift credentials with a validity of 3600 seconds and the given parameters.

    Args:
        :region_name: the AWS region name
        :cluster_identifier: the Redshift cluster identifier
        :user: the Redshift user to get credentials for
        :database: the Redshift database

    Returns:
        user, password
    """

    client = boto3.client('redshift', region_name=region_name)
    credential = client.get_cluster_credentials(
        DbUser=user,
        DbName=database,
        ClusterIdentifier=cluster_identifier,
        DurationSeconds=3600,
        AutoCreate=False
    )
    return credential['DbUser'], credential['DbPassword']


def get_flink_conf_dir():
    """
    Returns the Flink configuration directory.

    Returns:
        The Flink config dir path.
    """
    if constants.ENV_VARIABLES.FLINK_CONF_DIR in os.environ:
        return os.environ[constants.ENV_VARIABLES.FLINK_CONF_DIR]


def get_flink_lib_dir():
    """
    Returns the Flink libraries directory.

    Returns:
        The Flink libraries dir path.
    """
    if constants.ENV_VARIABLES.FLINK_LIB_DIR in os.environ:
        return os.environ[constants.ENV_VARIABLES.FLINK_LIB_DIR]


def get_hadoop_home():
    """
    Returns the Hadoop home directory.

    Returns:
        The Hadoop home dir path.
    """
    if constants.ENV_VARIABLES.HADOOP_HOME in os.environ:
        return os.environ[constants.ENV_VARIABLES.HADOOP_HOME]


def get_hadoop_classpath_glob():
    """
    Returns the Hadoop glob classpath.

    Returns:
        The Hadoop glob classpath.
    """
    if constants.ENV_VARIABLES.HADOOP_CLASSPATH_GLOB in os.environ:
        return os.environ[constants.ENV_VARIABLES.HADOOP_CLASSPATH_GLOB]


def _validate_enable_online_featuregroup_schema(featuregroup_schema):
    """
    Validates the user-provided schema of an online feature group
    Args:
        :featuregroup_schema: the schema dict to validate

    Returns:
        schema with default values
    """
    if featuregroup_schema == None or len(featuregroup_schema) == 0:
        raise ValueError("The feature schema is invalid, featuregroup schema is empty: {} ".format(featuregroup_schema))
    primary_idx = -1
    for idx, feature_def in enumerate(featuregroup_schema):
        if constants.REST_CONFIG.JSON_FEATURE_DESCRIPTION not in feature_def or \
                        feature_def[constants.REST_CONFIG.JSON_FEATURE_DESCRIPTION] is None:
            feature_def[constants.REST_CONFIG.JSON_FEATURE_DESCRIPTION] = "-"
        if constants.REST_CONFIG.JSON_FEATURE_PARTITION not in feature_def or \
                feature_def[constants.REST_CONFIG.JSON_FEATURE_PARTITION] is None:
            feature_def[constants.REST_CONFIG.JSON_FEATURE_PARTITION] = False
        if constants.REST_CONFIG.JSON_FEATURE_PRIMARY in feature_def and \
                        feature_def[constants.REST_CONFIG.JSON_FEATURE_PRIMARY] is not None:
            primary_idx = idx
        if constants.REST_CONFIG.JSON_FEATURE_ONLINE_TYPE not in feature_def or \
                        feature_def[constants.REST_CONFIG.JSON_FEATURE_ONLINE_TYPE] is None:
            if constants.REST_CONFIG.JSON_FEATURE_TYPE not in feature_def or \
                            feature_def[constants.REST_CONFIG.JSON_FEATURE_TYPE] is None:
                feature_def[constants.REST_CONFIG.JSON_FEATURE_ONLINE_TYPE] = \
                    feature_def[constants.REST_CONFIG.JSON_FEATURE_TYPE]
            else:
                raise ValueError("The feature schema is invalid, the feature definition: {} "
                                 "does not contain a type".format(feature_def))

        if constants.REST_CONFIG.JSON_FEATURE_NAME not in feature_def or \
                        feature_def[constants.REST_CONFIG.JSON_FEATURE_NAME] is None:
            raise ValueError("The feature schema is invalid, the feature definition: {} "
                             "does not contain a name".format(feature_def))
    if primary_idx == -1:
        raise ValueError("You must mark at least one feature as primary in the online feature group")

    return featuregroup_schema

def num_executors():
    """
    Get the number of executors configured for Jupyter

    Returns:
        Number of configured executors for Jupyter
    """
    sc = _find_spark().sparkContext
    try:
        return int(sc._conf.get("spark.dynamicAllocation.maxExecutors"))
    except:
        raise RuntimeError('Failed to find spark.dynamicAllocation.maxExecutors property, please select your mode as either Experiment, Parallel Experiments or Distributed Training.')

def num_param_servers():
    """
    Get the number of parameter servers configured for Jupyter

    Returns:
        Number of configured parameter servers for Jupyter
    """
    sc = _find_spark().sparkContext
    try:
        return int(os.environ['NUM_TF_PS'])
    except:
        return 0

def _find_spark():
    """
    Returns: SparkSession
    """
    return SparkSession.builder.getOrCreate()

def _on_executor_exit(signame):
    """
    Return a function to be run in a child process which will trigger
    SIGNAME to be sent when the parent process dies

    Args:
        :signame: the signame to send

    Returns:
        set_parent_exit_signal
    """
    signum = getattr(signal, signame)
    def set_parent_exit_signal():
        # http://linux.die.net/man/2/prctl

        PR_SET_PDEATHSIG = 1
        result = cdll['libc.so.6'].prctl(PR_SET_PDEATHSIG, signum)
        if result != 0:
            raise Exception('prctl failed with error code %s' % result)
    return set_parent_exit_signal


def _assumed_role():
    client = boto3.client('sts')
    response = client.get_caller_identity()
    # arns for assumed roles in SageMaker follow the following schema
    # arn:aws:sts::123456789012:assumed-role/my-role-name/my-role-session-name
    local_identifier = response['Arn'].split(':')[-1].split('/')
    if len(local_identifier) != 3 or local_identifier[0] != 'assumed-role':
        raise Exception(
            'Failed to extract assumed role from arn: ' + response['Arn'])
    return local_identifier[1]

def _get_region():
    if (os.environ[constants.ENV_VARIABLES.REGION_NAME_ENV_VAR] != constants.AWS.DEFAULT_REGION):
        return os.environ[constants.ENV_VARIABLES.REGION_NAME_ENV_VAR]
    else:
        return None

def _query_secrets_manager(secret_key):
    secret_name = 'hopsworks/role/' + _assumed_role()
    args = {'service_name': 'secretsmanager'}
    region_name = _get_region()
    if region_name:
        args['region_name'] = region_name
    client = boto3.client(**args)
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    return json.loads(get_secret_value_response['SecretString'])[secret_key]

def _query_parameter_store(secret_key):
    args = {'service_name': 'ssm'}
    region_name = _get_region()
    if region_name:
        args['region_name'] = region_name
    client = boto3.client(**args)
    name = '/hopsworks/role/' + _assumed_role() + '/type/' + secret_key
    return client.get_parameter(Name=name, WithDecryption=True)['Parameter']['Value']

def get_secret(secrets_store, secret_key=None, api_key_file=None):
    """
    Returns secret value from the AWS Secrets Manager or Parameter Store

    Args:
        :secrets_store: the underlying secrets storage to be used, e.g. `secretsmanager` or `parameterstore`
        :secret_type (str): key for the secret value, e.g. `api-key`, `cert-key`, `trust-store`, `key-store`
        :api_token_file: path to a file containing an api key
    Returns:
        :str: secret value
    """
    if secrets_store == constants.AWS.SECRETS_MANAGER:
        return _query_secrets_manager(secret_key)
    elif secrets_store == constants.AWS.PARAMETER_STORE:
        return _query_parameter_store(secret_key)
    elif secrets_store == constants.LOCAL.LOCAL_STORE:
        if not api_key_file:
            raise Exception('api_key_file needs to be set for local mode')
        try:
            with open(api_key_file) as f:
                return f.readline().strip()
        except:
            raise APIKeyFileNotFound('API Key fiel could not be read or was not found')
    else:
        raise UnkownSecretStorageError(
            "Secrets storage " + secrets_store + " is not supported.")

def write_b64_cert_to_bytes(b64_string, path):
    """Converts b64 encoded certificate to bytes file .

    Args:
        :b64_string (str): b64 encoded string of certificate
        :path (str): path where file is saved, including file name. e.g. /path/key-store.jks
    """

    with open(path, 'wb') as f:
        cert_b64 = base64.b64decode(b64_string)
        f.write(cert_b64)

def attach_jupyter_configuration_to_notebook(kernel_id):
    method = constants.HTTP_CONFIG.HTTP_PUT
    resource_url = constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   "project" + constants.DELIMITERS.SLASH_DELIMITER + os.environ["HOPSWORKS_PROJECT_ID"] + \
                   constants.DELIMITERS.SLASH_DELIMITER + "jupyter/attachConfiguration" + \
                   constants.DELIMITERS.SLASH_DELIMITER + os.environ["HADOOP_USER_NAME"] + \
                   constants.DELIMITERS.SLASH_DELIMITER + kernel_id
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    response = send_request(method, resource_url, headers=headers)
    if response.status_code >= 400:
        response_object = response.json()
        error_code, error_msg, user_msg = _parse_rest_error(response_object)
        raise Exception(error_msg)
