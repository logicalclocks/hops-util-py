"""

Miscellaneous utility functions for user applications.

"""

import os
import signal
from ctypes import cdll
import itertools
import socket
import json
from datetime import datetime
import time
from hops import hdfs
from hops import version
from hops import constants
import ssl

#! Needed for hops library backwards compatability
try:
    import requests
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

try:
    import http.client as http
except ImportError:
    import httplib as http

# in case importing in %%local
try:
    from pyspark.sql import SparkSession
except:
    pass

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

def _get_http_connection(https=False):
    """
    Opens a HTTP(S) connection to Hopsworks

    Args:
        https: boolean flag whether to use Secure HTTP or regular HTTP

    Returns:
        HTTP(S)Connection
    """
    host_port_pair = _get_host_port_pair()
    if (https):
        PROTOCOL = ssl.PROTOCOL_TLSv1_2
        ssl_context = ssl.SSLContext(PROTOCOL)
        connection = http.HTTPSConnection(str(host_port_pair[0]), int(host_port_pair[1]), context = ssl_context)
    else:
        connection = http.HTTPConnection(str(host_port_pair[0]), int(host_port_pair[1]))
    return connection

def set_auth_header(headers):
    if os.environ[constants.ENV_VARIABLES.REMOTE_ENV_VAR]:
        headers[constants.HTTP_CONFIG.HTTP_AUTHORIZATION] = "ApiKey " + get_api_key_aws(hdfs.project_name())
    else:
        headers[constants.HTTP_CONFIG.HTTP_AUTHORIZATION] = "Bearer " + get_jwt()

def send_request(connection, method, resource, body=None, headers=None):
    """
    Sends a request to Hopsworks. In case of Unauthorized response, submit the request once more as jwt might not
    have been read properly from local container.

    Args:
        connection: HTTP connection instance to Hopsworks
        method: HTTP(S) method
        resource: Hopsworks resource
        body: HTTP(S) body
        headers: HTTP(S) headers

    Returns:
        HTTP(S) response
    """
    if headers is None:
        headers = {}
    set_auth_header(headers)
    connection.request(method, resource, body, headers)
    response = connection.getresponse()
    if response.status == constants.HTTP_CONFIG.HTTP_UNAUTHORIZED:
        set_auth_header(headers)
        connection.request(method, resource, body, headers)
        response = connection.getresponse()
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
    session = requests.Session()

    retries = 3
    resp=None
    while retries > 0:
        resp = session.put("http://" + elastic_endpoint + "/" +  project.lower() + "_experiments/experiments/" + appid + "_" + str(elastic_id), data=json_data, headers=headers, verify=False)
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
    else:
        None


def get_jwt():
    """
    Retrieves jwt from local container

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
    client = session.client(
        service_name='secretsmanager'
    )
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    return json.loads(get_secret_value_response['SecretString'])['api-key']

def abspath(hdfs_path):
    if os.environ[constants.ENV_VARIABLES.REMOTE_ENV_VAR]:
        return hdfs_path
    else:
        return pydoop.path.abspath(hdfs_path)
