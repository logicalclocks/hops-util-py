"""

Miscellaneous utility functions for user applications.

"""

import os
import signal
from ctypes import cdll
import itertools
import socket
import json
import base64
from datetime import datetime
import time
from hops import hdfs
from hops import version
from pyspark.sql import SparkSession
from hops import constants
import ssl

#! Needed for hops library backwards compatability
try:
    import requests
except:
    pass
import pydoop.hdfs

try:
    import tensorflow
except:
    pass

try:
    import http.client as http
except ImportError:
    import httplib as http


def _get_elastic_endpoint():
    """

    Returns:

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

    """
    elastic_endpoint = os.environ[constants.ENV_VARIABLES.REST_ENDPOINT_END_VAR]
    return elastic_endpoint

hopsworks_endpoint = None
try:
    hopsworks_endpoint = _get_hopsworks_rest_endpoint()
except:
    pass

def _find_in_path(path, file):
    """

    Args:
        :path:
        :file:

    Returns:

    """
    for p in path.split(os.pathsep):
        candidate = os.path.join(p, file)
        if (os.path.exists(os.path.join(p, file))):
            return candidate
    return False

def _find_tensorboard():
    """

    Returns:
         tb_path
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
        :signame:

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

def num_executors():
    """
    Get the number of executors configured for Jupyter

    Returns:
        Number of configured executors for Jupyter
    """
    sc = _find_spark().sparkContext
    return int(sc._conf.get("spark.executor.instances"))

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
        x
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    return s.getsockname()[0]

def _time_diff(task_start, task_end):
    """
    Args:
        :task_start:
        :tast_end:

    Returns:

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
    Args:
        :project:
        :appid:
        :elastic_id:
        :json_data:

    Returns:

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
                       'executors': str(sc._conf.get("spark.executor.instances")),
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

    Args:
        best_param:

    Returns:

    """
    best_param_dict={}
    for hp in best_param:
        hp = hp.split('=')
        best_param_dict[hp[0]] = hp[1]

    return best_param_dict

def _find_spark():
    """

    Returns:

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