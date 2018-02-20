"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import os
from hops import hdfs as hopshdfs
from hops import tensorboard
from hops import devices
import pydoop.hdfs
import subprocess
import sys
import threading
import time
import six

run_id = 0

def launch(spark_session, map_fun, args_dict=None):
    """ Run the wrapper function with each hyperparameter combination as specified by the dictionary

    Args:
      :spark_session: SparkSession object
      :map_fun: The TensorFlow function to run
      :args_dict: (optional) A dictionary containing hyperparameter values to insert as arguments for each TensorFlow job
    """

    sc = spark_session.sparkContext
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

    #Each TF task should be run on 1 executor
    nodeRDD = sc.parallelize(range(num_executions), num_executions)

    #Force execution on executor, since GPU is located on executor
    global run_id
    nodeRDD.foreachPartition(_prepare_func(app_id, run_id, map_fun, args_dict))

    print('Finished TensorFlow job \n')
    print('Make sure to check /Logs/TensorFlow/' + app_id + '/runId.' + str(run_id) + ' for logfile and TensorBoard logdir')

    run_id += 1

    return 'hdfs:///Projects/' + hopshdfs.project_name() + '/Logs/TensorFlow/' + app_id

#Helper to put Spark required parameter iter in function signature
def _prepare_func(app_id, run_id, map_fun, args_dict):

    def _wrapper_fun(iter):

        for i in iter:
            executor_num = i

        tb_pid = 0
        tb_hdfs_path = ''

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
                hdfs_exec_logdir, hdfs_appid_logdir = hopshdfs.create_directories(app_id, run_id, param_string)
                pydoop.hdfs.dump('', os.environ['EXEC_LOGFILE'], user=hopshdfs.project_user())
                hopshdfs.init_logger()
                hopshdfs.log('Starting Spark executor with arguments ' + param_string)
                tb_hdfs_path, tb_pid = tensorboard.register(hdfs_exec_logdir, hdfs_appid_logdir, executor_num)

                gpu_str = '\nChecking for GPUs in the environment' + devices.get_gpu_info()
                hopshdfs.log(gpu_str)
                print(gpu_str)
                map_fun(*args)
            else:
                hopshdfs.log('Starting Spark executor')
                hdfs_exec_logdir, hdfs_appid_logdir = hopshdfs.create_directories(app_id, run_id, 'no_args')
                pydoop.hdfs.dump('', os.environ['EXEC_LOGFILE'], user=hopshdfs.project_user())
                hopshdfs.init_logger()
                tb_hdfs_path, tb_pid = tensorboard.register(hdfs_exec_logdir, hdfs_appid_logdir, executor_num)
                gpu_str = '\nChecking for GPUs in the environment' + devices.get_gpu_info()
                hopshdfs.log(gpu_str)
                print(gpu_str)
                map_fun()
        except:
            #Always do cleanup
            cleanup(tb_hdfs_path)
            if devices.get_num_gpus() > 0:
                t.do_run = False
                t.join()
            raise
        hopshdfs.log('Finished running')
        cleanup(tb_hdfs_path)
        if devices.get_num_gpus() > 0:
            t.do_run = False
            t.join()

    return _wrapper_fun

def cleanup(tb_hdfs_path):
    handle = hopshdfs.get()
    if not tb_hdfs_path == None and not tb_hdfs_path == '' and handle.exists(tb_hdfs_path):
        handle.delete(tb_hdfs_path)
    hopshdfs.kill_logger()