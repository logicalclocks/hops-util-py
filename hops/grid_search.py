"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import os
from hops import hdfs as hopshdfs
from hops import tensorboard
from hops import devices
from hops import util

import pydoop.hdfs
import threading
import six
import datetime

run_id = 0

def _grid_launch(sc, map_fun, args_dict, direction='max', local_logdir=False):
    """ Run the wrapper function with each hyperparameter combination as specified by the dictionary

    Args:
      :spark_session: SparkSession object
      :map_fun: The TensorFlow function to run
      :args_dict: (optional) A dictionary containing hyperparameter values to insert as arguments for each TensorFlow job
    """
    global run_id
    app_id = str(sc.applicationId)
    num_executions = 1

    if direction != 'max' and direction != 'min':
        raise ValueError('Invalid direction ' + direction +  ', must be max or min')

    arg_lists = list(args_dict.values())
    currentLen = len(arg_lists[0])
    for i in range(len(arg_lists)):
        if currentLen != len(arg_lists[i]):
            raise ValueError('Length of each function argument list must be equal')
        num_executions = len(arg_lists[i])

    #Each TF task should be run on 1 executor
    nodeRDD = sc.parallelize(range(num_executions), num_executions)

    #Force execution on executor, since GPU is located on executor
    job_start = datetime.datetime.now()
    nodeRDD.foreachPartition(_prepare_func(app_id, run_id, map_fun, args_dict, local_logdir))
    job_end = datetime.datetime.now()

    job_time_str = util.time_diff(job_start, job_end)

    arg_count = six.get_function_code(map_fun).co_argcount
    arg_names = six.get_function_code(map_fun).co_varnames
    hdfs_appid_dir = hopshdfs.get_experiments_dir() + '/' + app_id
    hdfs_runid_dir = hdfs_appid_dir + '/grid_search/run.' + str(run_id)

    max_val, max_hp, min_val, min_hp, avg = _get_best(args_dict, num_executions, arg_names, arg_count, hdfs_appid_dir, run_id)

    param_combination = ""
    best_val = ""

    if direction == 'max':
        param_combination = max_hp
        best_val = str(max_val)
        results = '\n------ Grid search results ------ direction(' + direction + ') \n' \
          'BEST combination ' + max_hp + ' -- metric ' + str(max_val) + '\n' \
          'WORST combination ' + min_hp + ' -- metric ' + str(min_val) + '\n' \
          'AVERAGE metric -- ' + str(avg) + '\n' \
          'Total job time ' + job_time_str + '\n'
        write_result(hdfs_runid_dir, results)
        print(results)
    elif direction == 'min':
        param_combination = min_hp
        best_val = str(min_val)
        results = '\n------ Grid search results ------ direction(' + direction + ') \n' \
        'BEST combination ' + min_hp + ' -- metric ' + str(min_val) + '\n' \
        'WORST combination ' + max_hp + ' -- metric ' + str(max_val) + '\n' \
        'AVERAGE metric -- ' + str(avg) + '\n' \
        'Total job time ' + job_time_str + '\n'
        write_result(hdfs_runid_dir, results)
        print(results)


    print('Finished Experiment \n')

    return hdfs_runid_dir, param_combination, best_val

def get_logdir(app_id):
    global run_id
    return hopshdfs.get_experiments_dir() + '/' + app_id + '/grid_search/run.' + str(run_id)



def write_result(runid_dir, string):
    metric_file = runid_dir + '/summary'
    fs_handle = hopshdfs.get_fs()
    try:
        fd = fs_handle.open_file(metric_file, mode='w')
    except:
        fd = fs_handle.open_file(metric_file, flags='w')
    fd.write(string.encode())
    fd.flush()
    fd.close()

def _prepare_func(app_id, run_id, map_fun, args_dict, local_logdir):

    def _wrapper_fun(iter):

        for i in iter:
            executor_num = i

        tb_hdfs_path = ''
        hdfs_exec_logdir = ''

        t = threading.Thread(target=devices.print_periodic_gpu_utilization)
        if devices.get_num_gpus() > 0:
            t.start()

        try:
            #Arguments
            if args_dict:
                argcount = six.get_function_code(map_fun).co_argcount
                names = six.get_function_code(map_fun).co_varnames

                args = []
                argIndex = 0
                param_string = ''
                while argcount > 0:
                    #Get args for executor and run function
                    param_name = names[argIndex]
                    param_val = args_dict[param_name][executor_num]
                    param_string += str(param_name) + '=' + str(param_val) + '.'
                    args.append(param_val)
                    argcount -= 1
                    argIndex += 1
                param_string = param_string[:-1]
                hdfs_exec_logdir, hdfs_appid_logdir = hopshdfs.create_directories(app_id, run_id, param_string, 'grid_search')
                pydoop.hdfs.dump('', os.environ['EXEC_LOGFILE'], user=hopshdfs.project_user())
                hopshdfs.init_logger()
                tb_hdfs_path, tb_pid = tensorboard.register(hdfs_exec_logdir, hdfs_appid_logdir, executor_num, local_logdir=local_logdir)

                gpu_str = '\nChecking for GPUs in the environment' + devices.get_gpu_info()
                hopshdfs.log(gpu_str)
                print(gpu_str)
                print('-------------------------------------------------------')
                print('Started running task ' + param_string + '\n')
                hopshdfs.log('Started running task ' + param_string)
                task_start = datetime.datetime.now()
                retval = map_fun(*args)
                task_end = datetime.datetime.now()
                _handle_return(retval, hdfs_exec_logdir)
                time_str = 'Finished task ' + param_string + ' - took ' + util.time_diff(task_start, task_end)
                print('\n' + time_str)
                print('-------------------------------------------------------')
                hopshdfs.log(time_str)
        except:
            #Always do cleanup
            _cleanup(tb_hdfs_path)
            if devices.get_num_gpus() > 0:
                t.do_run = False
                t.join()
            raise
        finally:
            if local_logdir:
                local_tb = tensorboard.local_logdir_path
                util.store_local_tensorboard(local_tb, hdfs_exec_logdir)


        _cleanup(tb_hdfs_path)
        if devices.get_num_gpus() > 0:
            t.do_run = False
            t.join()

    return _wrapper_fun


def _get_best(args_dict, num_combinations, arg_names, arg_count, hdfs_appid_dir, run_id):

    max_hp = ''
    max_val = ''

    min_hp = ''
    min_val = ''

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
            param_string += str(param_name) + '=' + str(param_val) + '.'
            num_args -= 1
            argIndex += 1

        param_string = param_string[:-1]

        path_to_metric = hdfs_appid_dir + '/grid_search/run.' + str(run_id) + '/' + param_string + '/metric'

        metric = None

        with pydoop.hdfs.open(path_to_metric, "r") as fi:
            metric = float(fi.read())
            fi.close()

            if first:
                max_hp = param_string
                max_val = metric
                min_hp = param_string
                min_val = metric
                first = False

            if metric > max_val:
                max_val = metric
                max_hp = param_string
            if metric <  min_val:
                min_val = metric
                min_hp = param_string


        results.append(metric)

    avg = sum(results)/float(len(results))

    return max_val, max_hp, min_val, min_hp, avg


def _handle_return(val, hdfs_exec_logdir):
    try:
        test = int(val)
    except:
        raise ValueError('Your function needs to return a metric (number) which should be maximized or minimized')

    metric_file = hdfs_exec_logdir + '/metric'
    fs_handle = hopshdfs.get_fs()
    try:
        fd = fs_handle.open_file(metric_file, mode='w')
    except:
        fd = fs_handle.open_file(metric_file, flags='w')
    fd.write(str(float(val)).encode())
    fd.flush()
    fd.close()

def _cleanup(tb_hdfs_path):
    handle = hopshdfs.get()
    if not tb_hdfs_path == None and not tb_hdfs_path == '' and handle.exists(tb_hdfs_path):
        handle.delete(tb_hdfs_path)
    hopshdfs.kill_logger()