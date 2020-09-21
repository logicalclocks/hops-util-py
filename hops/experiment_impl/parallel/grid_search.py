"""
Gridsearch implementation
"""

from hops import hdfs, tensorboard, devices

from hops.experiment_impl.util import experiment_utils
from hops.experiment import Direction

import threading
import six
import time
import os

def _run(sc, train_fn, run_id, args_dict, direction=Direction.MAX, local_logdir=False, name="no-name", optimization_key=None):
    """
    Run the wrapper function with each hyperparameter combination as specified by the dictionary

    Args:
        sc:
        train_fn:
        args_dict:
        direction:
        local_logdir:
        name:

    Returns:

    """
    app_id = str(sc.applicationId)
    num_executions = 1

    if direction.upper() != Direction.MAX and direction.upper() != Direction.MIN:
        raise ValueError('Invalid direction ' + direction +  ', must be Direction.MAX or Direction.MIN')

    arg_lists = list(args_dict.values())
    currentLen = len(arg_lists[0])
    for i in range(len(arg_lists)):
        if currentLen != len(arg_lists[i]):
            raise ValueError('Length of each function argument list must be equal')
        num_executions = len(arg_lists[i])

    #Each TF task should be run on 1 executor
    nodeRDD = sc.parallelize(range(num_executions), num_executions)

    #Make SparkUI intuitive by grouping jobs
    sc.setJobGroup(os.environ['ML_ID'], "{} | Grid Search".format(name))

    #Force execution on executor, since GPU is located on executor
    nodeRDD.foreachPartition(_prepare_func(app_id, run_id, train_fn, args_dict, local_logdir, optimization_key))

    arg_count = six.get_function_code(train_fn).co_argcount
    arg_names = six.get_function_code(train_fn).co_varnames
    exp_dir = experiment_utils._get_logdir(app_id, run_id)

    max_val, max_hp, min_val, min_hp, avg, max_return_dict, min_return_dict = experiment_utils._get_best(args_dict, num_executions, arg_names, arg_count, exp_dir, optimization_key)

    param_combination = ""
    best_val = ""
    return_dict = {}

    if direction.upper() == Direction.MAX:
        param_combination = max_hp
        best_val = str(max_val)
        return_dict = max_return_dict
    elif direction.upper() == Direction.MIN:
        param_combination = min_hp
        best_val = str(min_val)
        return_dict = min_return_dict

    print('Finished Experiment \n')

    best_dir = exp_dir + '/' + param_combination

    return best_dir, experiment_utils._get_params_dict(best_dir), best_val, return_dict

def _prepare_func(app_id, run_id, train_fn, args_dict, local_logdir, optimization_key):
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
        hdfs_exec_logdir = ''

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
                experiment_utils._handle_return(retval, hdfs_exec_logdir, optimization_key, logfile)
                time_str = 'Finished task ' + param_string + ' - took ' + experiment_utils._time_diff(task_start, task_end)
                print(time_str)
                print('Returning metric ' + str(retval))
                print('-------------------------------------------------------')
        except:
            raise
        finally:
            experiment_utils._cleanup(tensorboard, t)

    return _wrapper_fun