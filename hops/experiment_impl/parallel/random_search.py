"""
Random Search implementation
"""

from hops.experiment_impl.util import experiment_utils
from hops import devices, tensorboard, hdfs
from hops.experiment import Direction

import threading
import six
import time
import random
import os

def _run(sc, train_fn, run_id, args_dict, samples, direction=Direction.MAX, local_logdir=False, name="no-name", optimization_key=None):
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

    arg_lists = list(args_dict.values())
    for i in range(len(arg_lists)):
       if len(arg_lists[i]) != 2:
           raise ValueError('Boundary list must contain exactly two elements, [lower_bound, upper_bound] for each hyperparameter')

    hp_names = args_dict.keys()

    random_dict = {}
    for hp in hp_names:
        lower_bound = args_dict[hp][0]
        upper_bound = args_dict[hp][1]

        assert lower_bound < upper_bound, "lower bound: " + str(lower_bound) + " must be less than upper bound: " + str(upper_bound)

        random_values = []

        if type(lower_bound) is int and type(upper_bound) is int:
            for i in range(samples):
                random_values.append(random.randint(lower_bound, upper_bound))
        elif (type(lower_bound) is float or type(lower_bound) is int) and (type(upper_bound) is float or type(upper_bound) is int):
            for i in range(samples):
                random_values.append(random.uniform(lower_bound, upper_bound))
        else:
            raise ValueError('Only float and int is currently supported')

        random_dict[hp] = random_values

    random_dict, new_samples = _remove_duplicates(random_dict, samples)

    sc.setJobGroup(os.environ['ML_ID'], "{} | Random Search".format(name))
    #Each TF task should be run on 1 executor
    nodeRDD = sc.parallelize(range(new_samples), new_samples)

    nodeRDD.foreachPartition(_prepare_func(app_id, run_id, train_fn, random_dict, local_logdir, optimization_key))

    arg_count = six.get_function_code(train_fn).co_argcount
    arg_names = six.get_function_code(train_fn).co_varnames
    exp_dir = experiment_utils._get_logdir(app_id, run_id)

    max_val, max_hp, min_val, min_hp, avg, max_return_dict, min_return_dict = experiment_utils._get_best(random_dict, new_samples, arg_names, arg_count, exp_dir, optimization_key)

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

def _remove_duplicates(random_dict, samples):
    hp_names = random_dict.keys()
    concatenated_hp_combs_arr = []
    for index in range(samples):
        separated_hp_comb = ""
        for hp in hp_names:
            separated_hp_comb = separated_hp_comb + str(random_dict[hp][index]) + "&"
        concatenated_hp_combs_arr.append(separated_hp_comb)

    entry_index = 0
    indices_to_skip = []
    for entry in concatenated_hp_combs_arr:
        inner_index = 0
        for possible_dup_entry in concatenated_hp_combs_arr:
            if entry == possible_dup_entry and inner_index > entry_index:
                indices_to_skip.append(inner_index)
            inner_index = inner_index + 1
        entry_index = entry_index + 1
    indices_to_skip = list(set(indices_to_skip))

    for hp in hp_names:
        index = 0
        pruned_duplicates_arr = []
        for random_value in random_dict[hp]:
            if index not in indices_to_skip:
                pruned_duplicates_arr.append(random_value)
            index = index + 1
        random_dict[hp] = pruned_duplicates_arr

    return random_dict, samples - len(indices_to_skip)

#Helper to put Spark required parameter iter in function signature
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
                hdfs_exec_logdir, hdfs_appid_logdir = experiment_utils._create_experiment_subdirectories(app_id, run_id, param_string, 'random_search', params=params)
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