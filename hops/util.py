"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
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
    elastic_endpoint = os.environ['ELASTIC_ENDPOINT']
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
    elastic_endpoint = os.environ['REST_ENDPOINT']
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

def find_tensorboard():
    """

    Returns:
         tb_path
    """
    pypath = os.getenv("PYSPARK_PYTHON")
    pydir = os.path.dirname(pypath)
    search_path = os.pathsep.join([pydir, os.environ['PATH'], os.environ['PYTHONPATH']])
    tb_path = _find_in_path(search_path, 'tensorboard')
    if not tb_path:
        raise Exception("Unable to find 'tensorboard' in: {}".format(search_path))
    return tb_path

def on_executor_exit(signame):
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
    return int(sc._conf.get("spark.tensorflow.num.ps"))

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

def get_ip_address():
    """
    Simple utility to get host IP address

    Returns:
        x
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    return s.getsockname()[0]

def time_diff(task_start, task_end):
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

def put_elastic(project, appid, elastic_id, json_data):
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
    while retries > 0:
        resp = session.put("http://" + elastic_endpoint + "/" +  project + "_experiments/experiments/" + appid + "_" + str(elastic_id), data=json_data, headers=headers, verify=False)
        if resp.status_code == 200:
            return
        else:
            time.sleep(20)
            retries = retries - 1

    raise RuntimeError("Failed to publish experiment json file to Elastic, it is possible Elastic is experiencing problems. "
                       "Please contact an administrator.")



def populate_experiment(sc, model_name, module, function, logdir, hyperparameter_space, versioned_resources, description):
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
    if 'HOPSWORKS_USER' in os.environ:
        user = os.environ['HOPSWORKS_USER']
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

def finalize_experiment(experiment_json, hyperparameter, metric):
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
        experiment_json['tensorflow'] = os.environ['TENSORFLOW_VERSION']

    experiment_json['hops_py'] = version.__version__
    experiment_json['hops'] = os.environ['HADOOP_VERSION']
    experiment_json['hopsworks'] = os.environ['HOPSWORKS_VERSION']
    experiment_json['cuda'] = os.environ['CUDA_VERSION']
    experiment_json['kafka'] = os.environ['KAFKA_VERSION']
    return experiment_json

def store_local_tensorboard(local_tb, hdfs_exec_logdir):
    """

    Args:
        :local_tb:
        :hdfs_exec_logdir:

    Returns:

    """
    tb_contents = os.listdir(local_tb)
    for entry in tb_contents:
        pydoop.hdfs.put(local_tb + '/' + entry, hdfs_exec_logdir)

def get_notebook_path():
    """

    Returns:

    """
    material_passwd = os.getcwd() + '/material_passwd'

    if not os.path.exists(material_passwd):
        raise AssertionError('material_passwd is not present in current working directory')

    with open(material_passwd) as f:
        keyStorePwd = f.read()

    k_certificate = os.getcwd() + '/k_certificate'

    if not os.path.exists(k_certificate):
        raise AssertionError('k_certificate is not present in current working directory')

    with open(k_certificate, 'rb') as f:
        keyStore = f.read()
        keyStore = base64.b64encode(keyStore)

    json_contents = {'keyStorePwd': keyStorePwd,
                     'keyStore': keyStore}

    json_data = json.dumps(json_contents)

    headers = {'Content-type': 'application/json'}
    session = requests.Session()
    resp = session.post(hopsworks_endpoint + '/hopsworks-api/api/appservice/notebook', data=json_data, headers=headers, verify=False)

    return os.environ['HDFS_BASE_DIR'] + resp['notebook_path']

def version_resources(versioned_resources, rundir):
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

def convert_to_dict(best_param):
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


