"""

Miscellaneous utility functions for user applications.

"""

import os
import signal
import urllib
from ctypes import cdll
import itertools
import socket
import json
from datetime import datetime
import time

import boto3

from hops import hdfs
from hops import version
from hops import constants

from OpenSSL import SSL
from cryptography import x509
from cryptography.x509.oid import NameOID
import idna
from socket import socket

verify = None
#! Needed for hops library backwards compatability
try:
    import requests
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.SecurityWarning)
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except:
    pass

# Compatibility with SageMaker
try:
    import pydoop.hdfs
except:
    pass

try:
    import tensorflow
except:
    pass

# in case importing in %%local
try:
    from pyspark.sql import SparkSession
except:
    pass

session = requests.session()

def _get_elastic_endpoint():
    """

    Returns:
        The endpoint for putting things into elastic search

    """
    elastic_endpoint = os.environ[constants.ENV_VARIABLES.ELASTIC_ENDPOINT_ENV_VAR]
    host, port = elastic_endpoint.split(':')
    return host + ':' + port

elastic_endpoint = None
try:
    elastic_endpoint = _get_elastic_endpoint()
except:
    pass


def _get_hopsworks_rest_endpoint():
    """

    Returns:
        The hopsworks REST endpoint for making requests to the REST API

    """
    return os.environ[constants.ENV_VARIABLES.REST_ENDPOINT_END_VAR]

hopsworks_endpoint = None
try:
    hopsworks_endpoint = _get_hopsworks_rest_endpoint()
except:
    pass

def _find_in_path(path, file):
    """
    Utility method for finding a filename-string in a path

    Args:
        :path: the path to search
        :file: the filename to search for

    Returns:
        True if the filename was found in the path, otherwise False

    """
    for p in path.split(os.pathsep):
        candidate = os.path.join(p, file)
        if (os.path.exists(os.path.join(p, file))):
            return candidate
    return False

def _find_tensorboard():
    """
    Utility method for finding the tensorboard binary

    Returns:
         tb_path, path to the binary
    """
    pypath = os.getenv("PYSPARK_PYTHON")
    pydir = os.path.dirname(pypath)
    search_path = os.pathsep.join([pydir, os.environ[constants.ENV_VARIABLES.PATH_ENV_VAR], os.environ[constants.ENV_VARIABLES.PYTHONPATH_ENV_VAR]])
    tb_path = _find_in_path(search_path, 'tensorboard')
    if not tb_path:
        raise Exception("Unable to find 'tensorboard' in: {}".format(search_path))
    return tb_path

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



def set_auth_header(headers):
    """
    Set authorization header for HTTP requests to Hopsworks, depending if setup is remote or not.

    Args:
        http headers
    """
    if constants.ENV_VARIABLES.REMOTE_ENV_VAR in os.environ:
        headers[constants.HTTP_CONFIG.HTTP_AUTHORIZATION] = "ApiKey " + get_api_key_aws(hdfs.project_name())
    else:
        headers[constants.HTTP_CONFIG.HTTP_AUTHORIZATION] = "Bearer " + get_jwt()


def get_requests_verify(hostname, port):
    """
    Get verification method for sending HTTP requests to Hopsworks.
    Credit to https://gist.github.com/gdamjan/55a8b9eec6cf7b771f92021d93b87b2c
    Returns:
        if env var HOPS_UTIL_VERIFY is not false
            then if hopsworks certificate is self-signed, return the path to the truststore (PEM)
            else if hopsworks is not self-signed, return true
        return false
    """
    if constants.ENV_VARIABLES.REQUESTS_VERIFY_ENV_VAR in os.environ and os.environ[
        constants.ENV_VARIABLES.REQUESTS_VERIFY_ENV_VAR] == 'true':

        hostname_idna = idna.encode(hostname)
        sock = socket()

        sock.connect((hostname, int(port)))
        ctx = SSL.Context(SSL.SSLv23_METHOD)
        ctx.check_hostname = False
        ctx.verify_mode = SSL.VERIFY_NONE

        sock_ssl = SSL.Connection(ctx, sock)
        sock_ssl.set_connect_state()
        sock_ssl.set_tlsext_host_name(hostname_idna)
        sock_ssl.do_handshake()
        cert = sock_ssl.get_peer_certificate()
        crypto_cert = cert.to_cryptography()
        sock_ssl.close()
        sock.close()

        try:
            commonname = crypto_cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value
            issuer = crypto_cert.issuer.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value
            if commonname == issuer and constants.ENV_VARIABLES.DOMAIN_CA_TRUSTSTORE_PEM_ENV_VAR in os.environ:
                return os.environ[constants.ENV_VARIABLES.DOMAIN_CA_TRUSTSTORE_PEM_ENV_VAR]
            else:
                return True
        except x509.ExtensionNotFound:
            return True

    return False


def send_request(method, resource, data=None, headers=None):
    """
    Sends a request to Hopsworks. In case of Unauthorized response, submit the request once more as jwt might not
    have been read properly from local container.
    Args:
        method: HTTP(S) method
        resource: Hopsworks resource
        data: HTTP(S) payload
        headers: HTTP(S) headers
        verify: Whether to verify the https request
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
    req = requests.Request(method, url, data=data, headers=headers)
    prepped = session.prepare_request(req)

    response = session.send(prepped, verify=verify)

    if response.status_code == constants.HTTP_CONFIG.HTTP_UNAUTHORIZED:
        set_auth_header(headers)
        prepped = session.prepare_request(req)
        response = session.send(prepped)
    return response


def num_executors():
    """
    Get the number of executors configured for Jupyter

    Returns:
        Number of configured executors for Jupyter
    """
    sc = _find_spark().sparkContext
    return int(sc._conf.get("spark.dynamicAllocation.maxExecutors"))

def num_param_servers():
    """
    Get the number of parameter servers configured for Jupyter

    Returns:
        Number of configured parameter servers for Jupyter
    """
    sc = _find_spark().sparkContext
    try:
        return int(sc._conf.get("spark.tensorflow.num.ps"))
    except:
        return 0

def grid_params(dict):
    """
    Generate all possible combinations (cartesian product) of the hyperparameter values

    Args:
        :dict:

    Returns:
        A new dictionary with a grid of all the possible hyperparameter combinations
    """
    keys = dict.keys()
    val_arr = []
    for key in keys:
        val_arr.append(dict[key])

    permutations = list(itertools.product(*val_arr))

    args_dict = {}
    slice_index = 0
    for key in keys:
        args_arr = []
        for val in list(zip(*permutations))[slice_index]:
            args_arr.append(val)
        slice_index += 1
        args_dict[key] = args_arr
    return args_dict

def _get_ip_address():
    """
    Simple utility to get host IP address

    Returns:
        ip address of current host
    """
    try:
        _, _, _, _, addr = socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET, socket.SOCK_STREAM)[0]
        return addr[0]
    except:
        return socket.gethostbyname(socket.getfqdn())

def _time_diff(task_start, task_end):
    """
    Utility method for computing and pretty-printing the time difference between two timestamps

    Args:
        :task_start: the starting timestamp
        :tast_end: the ending timestamp

    Returns:
        The time difference in a pretty-printed format

    """
    time_diff = task_end - task_start

    seconds = time_diff.seconds

    if seconds < 60:
        return str(int(seconds)) + ' seconds'
    elif seconds == 60 or seconds <= 3600:
        minutes = float(seconds) / 60.0
        return str(int(minutes)) + ' minutes, ' + str((int(seconds) % 60)) + ' seconds'
    elif seconds > 3600:
        hours = float(seconds) / 3600.0
        minutes = (hours % 1) * 60
        return str(int(hours)) + ' hours, ' + str(int(minutes)) + ' minutes'
    else:
        return 'unknown time'

def _put_elastic(project, appid, elastic_id, json_data):
    """
    Utility method for putting JSON data into elastic search

    Args:
        :project: the project of the user/app
        :appid: the YARN appid
        :elastic_id: the id in elastic
        :json_data: the data to put

    Returns:
        None

    """
    if not elastic_endpoint:
        return
    headers = {'Content-type': 'application/json'}
    session_elastic = requests.Session()
    retries = 3
    resp=None
    while retries > 0:
        resp = session_elastic.put("http://" + elastic_endpoint + "/" +  project.lower() + "_experiments/experiments/" + appid + "_" + str(elastic_id), headers=headers, data=json_data, verify=False)
        if resp.status_code == 200:
            return
        else:
            time.sleep(5)
            retries = retries - 1

    if resp != None:
        raise RuntimeError("Failed to publish experiment json file to Elastic. Response: " + str(resp) +
                           ". It is possible Elastic is experiencing problems. Please contact an administrator.")
    else:
        raise RuntimeError("Failed to publish experiment json file to Elastic." +
                           " It is possible Elastic is experiencing problems. Please contact an administrator.")



def _populate_experiment(sc, model_name, module, function, logdir, hyperparameter_space, versioned_resources, description):
    """
    Args:
         :sc:
         :model_name:
         :module:
         :function:
         :logdir:
         :hyperparameter_space:
         :versioned_resources:
         :description:

    Returns:

    """
    user = None
    if constants.ENV_VARIABLES.HOPSWORKS_USER_ENV_VAR in os.environ:
        user = os.environ[constants.ENV_VARIABLES.HOPSWORKS_USER_ENV_VAR]
    return json.dumps({'project': hdfs.project_name(),
                       'user': user,
                       'name': model_name,
                       'module': module,
                       'function': function,
                       'status':'RUNNING',
                       'app_id': sc.applicationId,
                       'start': datetime.now().isoformat(),
                       'memory_per_executor': str(sc._conf.get("spark.executor.memory")),
                       'gpus_per_executor': str(sc._conf.get("spark.executor.gpus")),
                       'executors': str(num_executors()),
                       'logdir': logdir,
                       'hyperparameter_space': hyperparameter_space,
                       'versioned_resources': versioned_resources,
                       'description': description})

def _finalize_experiment(experiment_json, hyperparameter, metric):
    """
    Args:
        :experiment_json:
        :hyperparameter:
        :metric:

    Returns:

    """
    experiment_json = json.loads(experiment_json)
    experiment_json['metric'] = metric
    experiment_json['hyperparameter'] = hyperparameter
    experiment_json['finished'] = datetime.now().isoformat()
    experiment_json['status'] = "SUCCEEDED"
    experiment_json = _add_version(experiment_json)

    return json.dumps(experiment_json)

def _add_version(experiment_json):
    experiment_json['spark'] = os.environ['SPARK_VERSION']

    try:
        experiment_json['tensorflow'] = tensorflow.__version__
    except:
        experiment_json['tensorflow'] = os.environ[constants.ENV_VARIABLES.TENSORFLOW_VERSION_ENV_VAR]

    experiment_json['hops_py'] = version.__version__
    experiment_json['hops'] = os.environ[constants.ENV_VARIABLES.HADOOP_VERSION_ENV_VAR]
    experiment_json['hopsworks'] = os.environ[constants.ENV_VARIABLES.HOPSWORKS_VERSION_ENV_VAR]
    experiment_json['cuda'] = os.environ[constants.ENV_VARIABLES.CUDA_VERSION_ENV_VAR]
    experiment_json['kafka'] = os.environ[constants.ENV_VARIABLES.KAFKA_VERSION_ENV_VAR]
    return experiment_json

def _store_local_tensorboard(local_tb, hdfs_exec_logdir):
    """

    Args:
        :local_tb:
        :hdfs_exec_logdir:

    Returns:

    """
    tb_contents = os.listdir(local_tb)
    for entry in tb_contents:
        pydoop.hdfs.put(local_tb + '/' + entry, hdfs_exec_logdir)

def _version_resources(versioned_resources, rundir):
    """

    Args:
        versioned_resources:
        rundir:

    Returns:

    """
    if not versioned_resources:
        return None
    pyhdfs_handle = hdfs.get()
    pyhdfs_handle.create_directory(rundir)
    endpoint_prefix = hdfs.project_path()
    versioned_paths = []
    for hdfs_resource in versioned_resources:
        if pydoop.hdfs.path.exists(hdfs_resource):
            pyhdfs_handle.copy(hdfs_resource, pyhdfs_handle, rundir)
            path, filename = os.path.split(hdfs_resource)
            versioned_paths.append(rundir.replace(endpoint_prefix, '') + '/' + filename)
        else:
            raise Exception('Could not find resource in specified path: ' + hdfs_resource)

    return ', '.join(versioned_paths)

def _convert_to_dict(best_param):
    """
    Utiliy method for converting best_param string to dict

    Args:
        :best_param: the best_param string

    Returns:
        a dict with param->value

    """
    best_param_dict={}
    for hp in best_param:
        hp = hp.split('=')
        best_param_dict[hp[0]] = hp[1]

    return best_param_dict

def _find_spark():
    """

    Returns: SparkSession

    """
    return SparkSession.builder.getOrCreate()

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
    with open(constants.REST_CONFIG.JWT_TOKEN, "r") as jwt:
        return jwt.read()

def get_api_key_aws(project_name):
    import boto3

    def assumed_role():
        client = boto3.client('sts')
        response = client.get_caller_identity()
        # arns for assumed roles in SageMaker follow the following schema
        # arn:aws:sts::123456789012:assumed-role/my-role-name/my-role-session-name
        local_identifier = response['Arn'].split(':')[-1].split('/')
        if len(local_identifier) != 3 or local_identifier[0] != 'assumed-role':
            raise Exception('Failed to extract assumed role from arn: ' + response['Arn'])
        return local_identifier[1]

    secret_name = 'hopsworks/project/' + project_name + '/role/' + assumed_role()

    session = boto3.session.Session()
    if (os.environ[constants.ENV_VARIABLES.REGION_NAME_ENV_VAR] != constants.AWS.DEFAULT_REGION):
        region_name = os.environ[constants.ENV_VARIABLES.REGION_NAME_ENV_VAR]
    else:
        region_name = session.region_name

    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    return json.loads(get_secret_value_response['SecretString'])['api-key']

def abspath(hdfs_path):
    if constants.ENV_VARIABLES.REMOTE_ENV_VAR in os.environ:
        return hdfs_path
    else:
        return pydoop.hdfs.path.abspath(hdfs_path)

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
