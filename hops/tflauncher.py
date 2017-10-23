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

run_id = 0

def launch(spark_session, map_fun, args_dict=None):

    print('Starting TensorFlow job')

    #Temporary crap fix
    os.environ['CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['CLASSPATH']
    os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['SPARK_DIST_CLASSPATH']
    os.environ['CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.jar:" + os.environ['CLASSPATH']
    os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.jar:" + os.environ['SPARK_DIST_CLASSPATH']
    os.environ['CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['CLASSPATH']
    os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['SPARK_DIST_CLASSPATH']
    #os.environ['HADOOP_CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.jar:" + os.environ['HADOOP_CLASSPATH']

    sc = spark_session.sparkContext
    app_id = str(sc.applicationId)

    if args_dict == None:
        num_executors = 1
    else:
        arg_lists = args_dict.values()
        currentLen = len(arg_lists[0])
        for i in range(len(arg_lists)):
            if currentLen != len(arg_lists[i]):
                raise ValueError('Length of each function argument list must be equal')
            currentLen = len(arg_lists[i])

        num_executors = currentLen
        conf_num = int(sc._conf.get("spark.executor.instances"))
        if conf_num < num_executors:
            raise ValueError(('You configured {0} executors, but trying to run {1} TensorFlow jobs'
                              + ' increase number of executors or reduce number of TensorFlow jobs.')
                             .format(conf_num, num_executors))

    #Each TF task should be run on 1 executor
    nodeRDD = sc.parallelize(range(num_executors), num_executors)

    #Force execution on executor, since GPU is located on executor
    nodeRDD.foreachPartition(prepare_func(app_id, run_id, map_fun, args_dict))

    global run_id
    run_id += 1

    print('Finished TensorFlow job')

#Helper to put Spark required parameter iter in function signature
def prepare_func(app_id, run_id, map_fun, args_dict):

    def _wrapper_fun(iter):

        for i in iter:
            executor_num = i

        #Temporary crap fix
        os.environ['CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['CLASSPATH']
        os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['SPARK_DIST_CLASSPATH']
        os.environ['CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.jar:" + os.environ['CLASSPATH']
        os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.jar:" + os.environ['SPARK_DIST_CLASSPATH']
        os.environ['CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['CLASSPATH']
        os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['SPARK_DIST_CLASSPATH']
        #os.environ['HADOOP_CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.jar:" + os.environ['HADOOP_CLASSPATH']

        os.environ['HADOOP_PROXY_USER'] = os.environ['HDFS_USER']

        hdfs_exec_logdir, hdfs_appid_logdir = hopshdfs.create_directories(app_id, run_id, executor_num)
        tb_pid = 0
        tb_hdfs_path = ''

        try:
            #Arguments
            if args_dict:
                argcount = map_fun.func_code.co_argcount
                names = map_fun.func_code.co_varnames

                args = []
                argIndex = 0
                param_string = ''
                while argcount > 0:
                    #Get args for executor and run function
                    args_dict[names[argIndex]][executor_num]
                    param_name = names[argIndex]
                    param_val = args_dict[param_name][executor_num]
                    param_string += str(param_name) + '=' + str(param_val) + '.'
                    args.append(param_val)
                    argcount -= 1
                    argIndex += 1
                param_string = param_string[:-1]
                pydoop.hdfs.dump('', os.environ['EXEC_LOGFILE'], user=hopshdfs.project_user())
                hopshdfs.init_logger()
                hopshdfs.log('Starting Spark executor with arguments ' + param_string)
                tb_hdfs_path, tb_pid = tensorboard.register(hdfs_exec_logdir, hdfs_appid_logdir, executor_num, param_string=param_string)
                gpu_str = '\nChecking for GPUs in the environment' + devices.get_gpu_info()
                hopshdfs.log(gpu_str)
                print(gpu_str)
                map_fun(*args)
            else:
                pydoop.hdfs.dump('', os.environ['EXEC_LOGFILE'], user=hopshdfs.project_user())
                hopshdfs.init_logger()
                hopshdfs.log('Starting Spark executor')
                tb_hdfs_path, tb_pid = tensorboard.register(hdfs_exec_logdir, hdfs_appid_logdir, executor_num)
                gpu_str = '\nChecking for GPUs in the environment' + devices.get_gpu_info()
                hopshdfs.log(gpu_str)
                print(gpu_str)
                map_fun()

        except:
            #Always do cleanup
            cleanup(tb_pid, tb_hdfs_path)
            raise
        hopshdfs.log('Finished running')
        cleanup(tb_pid, tb_hdfs_path)

    return _wrapper_fun

def cleanup(tb_pid, tb_hdfs_path):
    hopshdfs.log('Performing cleanup')
    if tb_pid != 0:
        subprocess.Popen(["kill", str(tb_pid)])
        handle = hopshdfs.get()
        handle.delete(tb_hdfs_path)
        tensorboard.store()
        tensorboard.clean()
        hopshdfs.kill_logger()
