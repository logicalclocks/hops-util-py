"""
Simple experiment implementation
"""

from hops.experiment_impl.util import experiment_utils
from hops import devices, tensorboard, hdfs

import threading
import time
import json
import os
import six


def _run(sc, train_fn, run_id, args_dict=None, local_logdir=False, name="no-name"):
    """

    Args:
        sc:
        train_fn:
        args_dict:
        local_logdir:
        name:

    Returns:

    """

    app_id = str(sc.applicationId)


    if args_dict == None:
        num_executions = 1
    else:
        arg_lists = list(args_dict.values())
        currentLen = len(arg_lists[0])
        for i in range(len(arg_lists)):
            if currentLen != len(arg_lists[i]):
                raise ValueError('Length of each function argument list must be equal')
            num_executions = len(arg_lists[i])

    sc.setJobGroup(os.environ['ML_ID'], "{} | Launcher running experiment".format(name))
    #Each TF task should be run on 1 executor
    nodeRDD = sc.parallelize(range(num_executions), num_executions)

    #Force execution on executor, since GPU is located on executor
    nodeRDD.foreachPartition(_prepare_func(app_id, run_id, train_fn, args_dict, local_logdir))

    print('Finished Experiment \n')

    # For single run return .return if exists
    if args_dict == None:
        path_to_return = experiment_utils._get_logdir(app_id, run_id) + '/.outputs.json'
        if hdfs.exists(path_to_return):
            return_json = hdfs.load(path_to_return)
            return_dict = json.loads(return_json)
            return experiment_utils._get_logdir(app_id, run_id), return_dict
        else:
            return experiment_utils._get_logdir(app_id, run_id), None
    elif num_executions == 1:
        arg_count = six.get_function_code(train_fn).co_argcount
        arg_names = six.get_function_code(train_fn).co_varnames
        argIndex = 0
        param_string = ''
        while arg_count > 0:
            param_name = arg_names[argIndex]
            param_val = args_dict[param_name][0]
            param_string += str(param_name) + '=' + str(param_val) + '&'
            arg_count -= 1
            argIndex += 1
        param_string = param_string[:-1]
        path_to_return = experiment_utils._get_logdir(app_id, run_id) + '/' + param_string + '/.outputs.json'
        if hdfs.exists(path_to_return):
            return_json = hdfs.load(path_to_return)
            return_dict = json.loads(return_json)
            return experiment_utils._get_logdir(app_id, run_id), return_dict
        else:
            return experiment_utils._get_logdir(app_id, run_id), None
    else:
        return experiment_utils._get_logdir(app_id, run_id), None

#Helper to put Spark required parameter iter in function signature
def _prepare_func(app_id, run_id, train_fn, args_dict, local_logdir):
    """

    Args:
        app_id:
        run_id:
        train_fn:
        args_dict:
        local_logdir:

    Returns:

    """
    def _wrapper_fun(iter):
        """

        Args:
            iter:

        Returns:

        """

        for i in iter:
            executor_num = i

        experiment_utils._set_ml_id(app_id, run_id)

        tb_hdfs_path = ''

        hdfs_exec_logdir = experiment_utils._get_logdir(app_id, run_id)

        t = threading.Thread(target=devices._print_periodic_gpu_utilization)
        if devices.get_num_gpus() > 0:
            t.start()

        try:
            #Arguments
            if args_dict:
                param_string, params, args = experiment_utils.build_parameters(train_fn, executor_num, args_dict)
                hdfs_exec_logdir, hdfs_appid_logdir = experiment_utils._create_experiment_subdirectories(app_id, run_id, param_string, 'grid_search', params=params)
                logfile = experiment_utils._init_logger(hdfs_exec_logdir)
                tb_hdfs_path, tb_pid = tensorboard._register(hdfs_exec_logdir, hdfs_appid_logdir, executor_num, local_logdir=local_logdir)
                print(devices._get_gpu_info())
                print('-------------------------------------------------------')
                print('Started running task ' + param_string)
                task_start = time.time()
                retval = train_fn(*args)
                task_end = time.time()
                experiment_utils._handle_return_simple(retval, hdfs_exec_logdir, logfile)
                time_str = 'Finished task ' + param_string + ' - took ' + experiment_utils._time_diff(task_start, task_end)
                print(time_str)
                print('-------------------------------------------------------')
            else:
                tb_hdfs_path, tb_pid = tensorboard._register(hdfs_exec_logdir, hdfs_exec_logdir, executor_num, local_logdir=local_logdir)
                logfile = experiment_utils._init_logger(hdfs_exec_logdir)
                print(devices._get_gpu_info())
                print('-------------------------------------------------------')
                print('Started running task')
                task_start = time.time()
                retval = train_fn()
                task_end = time.time()
                experiment_utils._handle_return_simple(retval, hdfs_exec_logdir, logfile)
                time_str = 'Finished task - took ' + experiment_utils._time_diff(task_start, task_end)
                print(time_str)
                print('-------------------------------------------------------')
        except:
            raise
        finally:
            experiment_utils._cleanup(tensorboard, t)

    return _wrapper_fun