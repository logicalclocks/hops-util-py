"""
Utility functions for experiments
"""
from __future__ import print_function

import builtins as __builtin__

import os
import signal
from ctypes import cdll
import itertools
import socket
import json
import shutil
import subprocess
import datetime
import numpy as np
import six

from hops.exceptions import RestAPIError

from hops import constants
from hops import devices
from hops import util
from hops import hdfs

logger_fd = None

def _init_logger(exec_logdir, role=None, index=None):
    """
    Initialize the logger by opening the log file and pointing the global fd to the open file
    """

    prefix = ''
    if role is not None and index is not None:
        prefix = str(role) + '_' + str(index) + '_'

    logfile = exec_logdir + '/' + prefix + 'output.log'
    fs_handle = hdfs.get_fs()
    global logger_fd
    try:
        logger_fd = fs_handle.open_file(logfile, mode='w')
    except:
        logger_fd = fs_handle.open_file(logfile, flags='w')

    # save the builtin print
    original_print = __builtin__.print

    def experiment_print(*args, **kwargs):
        """Experiments custom print() function."""
        log(' '.join(str(x) for x in args))
        original_print(*args, **kwargs)

    # override the builtin print
    __builtin__.print = experiment_print

    return logfile.replace(hdfs.project_path(), '')

def log(log_entry):
    """
    Logs a string to the log file
    Args:
        :log_entry: string to log
    """
    global logger_fd
    if logger_fd:
        try:
            log_entry = str(log_entry)
            logger_fd.write(('{0}: {1}'.format(datetime.datetime.now().isoformat(), log_entry) + '\n').encode())
        except:
            logger_fd.write(('{0}: {1}'.format(datetime.datetime.now().isoformat(),
                                        'ERROR! Attempting to write a non-string object to logfile') + '\n').encode())

def _close_logger():
    """
    Closes the logfile
    """
    global logger_fd
    if logger_fd:
        try:
            log('Finished running task')
            logger_fd.flush()
            logger_fd.close()
        except:
            pass

def _handle_return(retval, hdfs_exec_logdir, optimization_key, logfile):
    """

    Args:
        val:
        hdfs_exec_logdir:

    Returns:

    """

    _upload_file_output(retval, hdfs_exec_logdir)

    # Validation
    if not optimization_key and type(retval) is dict and len(retval.keys()) > 1:
        raise Exception('Missing optimization_key argument, when returning multiple values in a dict the optimization_key argument must be set.')
    elif type(retval) is dict and optimization_key not in retval and len(retval.keys()) >= 1:
        raise Exception('optimization_key not in returned dict, when returning multiple values in a dict the optimization_key argument must be set to indicate which key the optimization algorithm should maximize or minimize on.')
    elif type(retval) is dict and len(retval.keys()) == 0:
        raise Exception('Returned dict is empty, must contain atleast 1 metric to maximize or minimize.')

    # Validate that optimization_key is a number
    if type(retval) is dict and len(retval.keys()) > 1:
        opt_val = retval[optimization_key]
        opt_val = _validate_optimization_value(opt_val)
        retval[optimization_key] = opt_val
    elif type(retval) is dict and len(retval.keys()) == 1:
        opt_val = retval[list(retval.keys())[0]]
        opt_val = _validate_optimization_value(opt_val)
        retval[list(retval.keys())[0]] = opt_val
    else:
        opt_val = _validate_optimization_value(retval)
        retval = {'metric': opt_val}

    retval['log'] = logfile

    return_file = hdfs_exec_logdir + '/.outputs.json'
    hdfs.dump(dumps(retval), return_file)

    metric_file = hdfs_exec_logdir + '/.metric'
    hdfs.dump(str(opt_val), metric_file)

def _validate_optimization_value(opt_val):
        try:
            int(opt_val)
            return opt_val
        except:
            pass
        try:
            float(opt_val)
            return opt_val
        except:
            pass
        raise ValueError('Metric to maximize or minimize is not a number: {}'.format(opt_val))

def _upload_file_output(retval, hdfs_exec_logdir):
    if type(retval) is dict:
        for metric_key in retval.keys():
            value = str(retval[metric_key])
            if '/' in value or os.path.exists(os.getcwd() + '/' + value):
                if os.path.exists(value): # absolute path
                    if hdfs.exists(hdfs_exec_logdir + '/' + value.split('/')[-1]):
                        hdfs.delete(hdfs_exec_logdir + '/' + value.split('/')[-1], recursive=False)
                    hdfs.copy_to_hdfs(value, hdfs_exec_logdir)
                    os.remove(value)
                    hdfs_exec_logdir = hdfs.abs_path(hdfs_exec_logdir)
                    retval[metric_key] = hdfs_exec_logdir[len(hdfs.abs_path(hdfs.project_path())):] + '/' +  value.split('/')[-1]
                elif os.path.exists(os.getcwd() + '/' + value): # relative path
                    output_file = os.getcwd() + '/' + value
                    if hdfs.exists(hdfs_exec_logdir + '/' + value):
                        hdfs.delete(hdfs_exec_logdir + '/' + value, recursive=False)
                    hdfs.copy_to_hdfs(value, hdfs_exec_logdir)
                    os.remove(output_file)
                    hdfs_exec_logdir = hdfs.abs_path(hdfs_exec_logdir)
                    retval[metric_key] = hdfs_exec_logdir[len(hdfs.abs_path(hdfs.project_path())):] + '/' +  output_file.split('/')[-1]
                elif value.startswith('Experiments') and value.endswith('output.log'):
                    continue
                elif value.startswith('Experiments') and hdfs.exists(hdfs.project_path() + '/' + value):
                    hdfs.cp(hdfs.project_path() + '/' + value, hdfs_exec_logdir)
                else:
                    raise Exception('Could not find file or directory on path ' + str(value))

def _handle_return_simple(retval, hdfs_exec_logdir, logfile):
    """

    Args:
        val:
        hdfs_exec_logdir:

    Returns:

    """
    return_file = hdfs_exec_logdir + '/.outputs.json'

    _upload_file_output(retval, hdfs_exec_logdir)

    # Validation
    if type(retval) is not dict:
        try:
            retval = {'metric': retval}
        except:
            pass

    retval['log'] = logfile

    hdfs.dump(dumps(retval), return_file)

def _cleanup(tensorboard, gpu_thread):

    print("Cleaning up... ")

    # Kill running TB
    try:
        if tensorboard.tb_pid != 0:
            subprocess.Popen(["kill", str(tensorboard.tb_pid)])
    except Exception as err:
        print('Exception occurred while killing tensorboard: {}'.format(err))
        pass

    # Store local TB in hdfs
    try:
        if tensorboard.local_logdir_bool and tensorboard.events_logdir:
            _store_local_tensorboard(tensorboard.local_logdir_path, tensorboard.events_logdir)
    except Exception as err:
        print('Exception occurred while uploading local logdir to hdfs: {}'.format(err))
        pass

    # Get rid of TensorBoard endpoint file
    try:
        handle = hdfs.get()
        if tensorboard.endpoint and handle.exists(tensorboard.endpoint):
            handle.delete(tensorboard.endpoint)
    except Exception as err:
        print('Exception occurred while deleting tensorboard endpoint file: {}'.format(err))
        pass
    finally:
        tensorboard._reset_global()

    # Close and logging fd and flush
    try:
        _close_logger()
    except Exception as err:
        print('Exception occurred while closing logger: {}'.format(err))
        pass

    # Stop the gpu monitoring thread
    try:
        gpu_thread.do_run = False
    except Exception as err:
        print('Exception occurred while stopping GPU monitoring thread: {}'.format(err))
        pass

def _store_local_tensorboard(local_tb_path, hdfs_exec_logdir):
    """

    Args:
        :local_tb:
        :hdfs_exec_logdir:

    Returns:

    """
    if os.path.exists(local_tb_path):
        tb_contents = os.listdir(local_tb_path)
        for entry in tb_contents:
            hdfs.copy_to_hdfs(local_tb_path + '/' + entry, hdfs_exec_logdir)
        try:
            shutil.rmtree(local_tb_path)
        except:
            pass

def _build_summary_json(logdir):

    combinations = []
    return_files = []
    hp_arr = None
    output_arr = None

    for experiment_dir in hdfs.ls(logdir):
        runs = hdfs.ls(experiment_dir, recursive=True)
        for run in runs:
            if run.endswith('.outputs.json'):
                return_files.append(run)

    for return_file in return_files:
        output_arr = _convert_return_file_to_arr(return_file)
        param_file = return_file.replace('outputs.json', 'hparams.json')
        if hdfs.exists(param_file):
            hp_arr = _convert_param_to_arr(param_file)
        combinations.append({'parameters': hp_arr, 'outputs': output_arr})

    return dumps({'combinations': combinations})

def _get_experiments_dir():
    """
    Gets the root folder where the experiments are writing their results

    Returns:
        The folder where the experiments are writing results
    """
    assert hdfs.exists(hdfs.project_path() + "Experiments"), "Your project is missing a dataset named Experiments, please create it."
    return hdfs.project_path() + "Experiments"

def _get_logdir(app_id, run_id):
    """

    Args:
        app_id: app_id for experiment
        run_id: run_id for experiment

    Returns:
        The folder where a particular experiment is writing results

    """
    return _get_experiments_dir() + '/' + str(app_id) + '_' + str(run_id)

def _create_experiment_subdirectories(app_id, run_id, param_string, type, sub_type=None, params=None):
    """
    Creates directories for an experiment, if Experiments folder exists it will create directories
    below it, otherwise it will create them in the Logs directory.

    Args:
        :app_id: YARN application ID of the experiment
        :run_id: Experiment ID
        :param_string: name of the new directory created under parent directories
        :type: type of the new directory parent, e.g differential_evolution
        :sub_type: type of sub directory to parent, e.g generation
        :params: dict of hyperparameters

    Returns:
        The new directories for the yarn-application and for the execution (hdfs_exec_logdir, hdfs_appid_logdir)
    """

    pyhdfs_handle = hdfs.get()

    hdfs_events_parent_dir = hdfs.project_path() + "Experiments"

    hdfs_experiment_dir = hdfs_events_parent_dir + "/" + app_id + "_" + str(run_id)

    # determine directory structure based on arguments
    if sub_type:
        hdfs_exec_logdir = hdfs_experiment_dir + "/" + str(sub_type) + '/' + str(param_string)
        if pyhdfs_handle.exists(hdfs_exec_logdir):
            hdfs.delete(hdfs_exec_logdir, recursive=True)
    elif not param_string and not sub_type:
        if pyhdfs_handle.exists(hdfs_experiment_dir):
            hdfs.delete(hdfs_experiment_dir, recursive=True)
        hdfs_exec_logdir = hdfs_experiment_dir + '/'
    else:
        hdfs_exec_logdir = hdfs_experiment_dir + '/' + str(param_string)
        if pyhdfs_handle.exists(hdfs_exec_logdir):
            hdfs.delete(hdfs_exec_logdir, recursive=True)

    # Need to remove directory if it exists (might be a task retry)

    # create the new directory
    pyhdfs_handle.create_directory(hdfs_exec_logdir)

    return_file = hdfs_exec_logdir + '/.hparams.json'
    hdfs.dump(dumps(params), return_file)

    return hdfs_exec_logdir, hdfs_experiment_dir

def _get_params_dict(best_dir):
    """
    Utiliy method for converting best_param string to dict

    Args:
        :best_param: the best_param string

    Returns:
        a dict with param->value

    """

    params_json = hdfs.load(best_dir + '/.hparams.json')
    params_dict = json.loads(params_json)
    return params_dict

def _convert_param_to_arr(params_file):
    params = hdfs.load(params_file)
    params_dict = json.loads(params)
    return params_dict

def _convert_return_file_to_arr(return_file_path):
    return_file_contents = hdfs.load(return_file_path)

    # Could be a number
    try:
        metric = int(return_file_contents)
        return [{'metric': metric}]
    except:
        pass

    return_json = json.loads(return_file_contents)
    metric_dict = {}
    for metric_key in return_json:
        metric_dict[metric_key] = return_json[metric_key]
    return metric_dict

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
    Args:
        :task_start: time in seconds
        :tast_end: time in seconds

    Returns:

    """

    millis = _seconds_to_milliseconds(task_end) - _seconds_to_milliseconds(task_start)
    millis = int(millis)
    seconds=(millis/1000)%60
    seconds = int(seconds)
    minutes=(millis/(1000*60))%60
    minutes = int(minutes)
    hours=(millis/(1000*60*60))%24

    return "%d hours, %d minutes, %d seconds" % (hours, minutes, seconds)

def _seconds_to_milliseconds(time):
    return int(round(time * 1000))

def _attach_experiment_xattr(ml_id, json_data, op_type):
    """
    Utility method for putting JSON data into elastic search

    Args:
        :ml_id: experiment id
        :json_data: the experiment json object
        :op_type: operation type INIT/MODEL_UPDATE/FULL_UPDATE

    Returns:
        None

    """

    json_data = dumps(json_data)

    headers = {'Content-type': 'application/json'}
    resource_url = constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_EXPERIMENTS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   ml_id + "?type=" + op_type

    response = util.send_request('PUT', resource_url, data=json_data, headers=headers)

    response_object = response.json()
    if response.status_code >= 400:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not create experiment (url: {}), server response: \n "
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))
    else:
        return response_object

def _attach_model_xattr(ml_id, json_data):
    """
    Utility method for putting JSON data into elastic search

    Args:
        :ml_id: the id of the model
        :json_data: the data to put

    Returns:
        None

    """
    headers = {'Content-type': 'application/json'}
    resource_url = constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_MODELS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   ml_id

    resp = util.send_request('PUT', resource_url, data=json_data, headers=headers)

def _populate_experiment(model_name, function, type, hp, description, app_id, direction, optimization_key):
    """
    Args:
         :sc:
         :model_name:
         :module:
         :function:
         :logdir:
         :hyperparameter_space:
         :description:

    Returns:

    """
    jobName=None
    if constants.ENV_VARIABLES.JOB_NAME_ENV_VAR in os.environ:
        jobName = os.environ[constants.ENV_VARIABLES.JOB_NAME_ENV_VAR]

    kernelId=None
    if constants.ENV_VARIABLES.KERNEL_ID_ENV_VAR in os.environ:
        kernelId = os.environ[constants.ENV_VARIABLES.KERNEL_ID_ENV_VAR]

    if model_name == 'no-name' and jobName:
        model_name = jobName

    return {'id': os.environ['ML_ID'], 'name': model_name, 'projectName': hdfs.project_name(), 'description': description,
            'state': 'RUNNING', 'function': function, 'experimentType': type, 'appId': app_id, 'direction': direction,
            'optimizationKey': optimization_key, 'jobName': jobName, 'kernelId': kernelId}

def _populate_experiment_model(model, project=None):
    """
    Args:
         :model:
         :project_name:

    Returns:

    """

    if project is None:
        project = hdfs.project_name()
    return {'id': os.environ['ML_ID'], 'model': model, 'modelProjectName': project }

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

def _get_best(args_dict, num_combinations, arg_names, arg_count, hdfs_appid_dir, optimization_key):

    if not optimization_key:
        optimization_key = 'metric'

    max_hp = ''
    max_val = ''

    min_hp = ''
    min_val = ''

    min_return_dict = {}
    max_return_dict = {}

    results = []

    first = True

    for i in range(num_combinations):

        argIndex = 0
        param_string = ''

        num_args = arg_count

        while num_args > 0:
            #Get args for executor and run function
            param_name = arg_names[argIndex]
            param_val = args_dict[param_name][i]
            param_string += str(param_name) + '=' + str(param_val) + '&'
            num_args -= 1
            argIndex += 1

        param_string = param_string[:-1]

        path_to_return = hdfs_appid_dir + '/' + param_string + '/.outputs.json'

        assert hdfs.exists(path_to_return), 'Could not find .return file on path: {}'.format(path_to_return)

        with hdfs.open_file(path_to_return, flags="r") as fi:
            return_dict = json.loads(fi.read())
            fi.close()

            # handle case when dict with 1 key is returned
            if optimization_key == 'metric' and len(return_dict.keys()) == 1:
                optimization_key = list(return_dict.keys())[0]

            metric = float(return_dict[optimization_key])

            if first:
                max_hp = param_string
                max_val = metric
                max_return_dict = return_dict
                min_hp = param_string
                min_val = metric
                min_return_dict = return_dict
                first = False

            if metric > max_val:
                max_val = metric
                max_hp = param_string
                max_return_dict = return_dict
            if metric <  min_val:
                min_val = metric
                min_hp = param_string
                min_return_dict = return_dict

        results.append(metric)

    avg = sum(results)/float(len(results))

    return max_val, max_hp, min_val, min_hp, avg, max_return_dict, min_return_dict

def _finalize_experiment(experiment_json, metric, app_id, run_id, state, duration, logdir, bestLogdir, optimization_key):

    summary_file = _build_summary_json(logdir)

    if summary_file:
        hdfs.dump(summary_file, logdir + '/.summary.json')

    if bestLogdir:
        experiment_json['bestDir'] = bestLogdir[len(hdfs.project_path()):]
    experiment_json['optimizationKey'] = optimization_key
    experiment_json['metric'] = metric
    experiment_json['state'] = state
    experiment_json['duration'] = duration
    exp_ml_id = app_id + "_" + str(run_id)
    _attach_experiment_xattr(exp_ml_id, experiment_json, 'FULL_UPDATE')

def _find_task_and_index(host_port, cluster_spec):
    """

    Args:
        host_port:
        cluster_spec:

    Returns:

    """
    index = 0
    for entry in cluster_spec["worker"]:
        if entry == host_port:
            return "worker", index
        index = index + 1

    index = 0
    for entry in cluster_spec["ps"]:
        if entry == host_port:
            return "ps", index
        index = index + 1


    if cluster_spec["chief"][0] == host_port:
        return "chief", 0

def _find_index(host_port, cluster_spec):
    """

    Args:
        host_port:
        cluster_spec:

    Returns:

    """
    index = 0
    for entry in cluster_spec["cluster"]["worker"]:
        if entry == host_port:
            return index
        else:
            index = index + 1
    return -1

def _set_ml_id(app_id, run_id):
    os.environ['HOME'] = os.getcwd()
    os.environ['ML_ID'] = str(app_id) + '_' + str(run_id)

def _get_metric(return_dict, metric_key):
    if return_dict and metric_key:
        assert metric_key in return_dict.keys(), 'Supplied metric_key {} is not in returned dict {}'.format(metric_key, return_dict)
        return str(return_dict[metric_key])
    elif return_dict is not None and len(return_dict.keys()) == 2 and 'metric' in return_dict.keys():
        return str(return_dict['metric'])
    else:
        return None

def json_default_numpy(obj):
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    else:
        raise TypeError(
            "Object of type {0}: {1} is not JSON serializable"
            .format(type(obj), obj))

def dumps(data):
    return json.dumps(data, default=json_default_numpy)

def build_parameters(map_fun, executor_num, args_dict):
    argcount = six.get_function_code(map_fun).co_argcount
    names = six.get_function_code(map_fun).co_varnames
    args = []
    argIndex = 0
    param_string = ''
    params = {}
    while argcount > 0:
        param_name = names[argIndex]
        param_val = args_dict[param_name][executor_num]
        param_string += str(param_name) + '=' + str(param_val) + '&'
        params[param_name] = param_val
        args.append(param_val)
        argcount -= 1
        argIndex += 1
    param_string = param_string[:-1]
    return param_string, params, args

def _create_experiment_dir(app_id, run_id):
    experiment_path = _get_logdir(app_id, run_id)

    if hdfs.exists(experiment_path):
        hdfs.delete(experiment_path, recursive=True)

    hdfs.mkdir(experiment_path)