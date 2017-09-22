"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import os
from hopsutil import hdfs as hopshdfs
import pydoop.hdfs.fs as pydoophdfs
from hopsutil import tensorboard
import subprocess
import datetime

run_id = 0

def launch(spark_session, map_fun, args_dict=None):

    #Temporary crap fix
    os.environ['CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['CLASSPATH']
    os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['SPARK_DIST_CLASSPATH']
    os.environ['CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.jar:" + os.environ['CLASSPATH']
    os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.jar:" + os.environ['SPARK_DIST_CLASSPATH']
    #os.environ['HADOOP_CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.jar:" + os.environ['HADOOP_CLASSPATH']

    sc = spark_session.sparkContext
    app_id = str(sc.applicationId)

    if args_dict == None:
        num_executors = 1
    else:
        num_executors = len(args_dict.values()[0])

    #TF task should be run on 1 executor
    nodeRDD = sc.parallelize(range(num_executors), num_executors)

    #Force execution on executor, since GPU is located on executor
    nodeRDD.foreachPartition(prepare_func(app_id, run_id, map_fun, args_dict))

    global run_id
    run_id += 1

#Helper to put Spark required parameter iter in function signature
def prepare_func(app_id, run_id, map_fun, args_dict):

    def _wrapper_fun(iter):

        for i in iter:
            executor_num = i

        #Temporary crap fix
        os.environ['CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['CLASSPATH']
        os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['SPARK_DIST_CLASSPATH']
        os.environ['CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.jar:" + os.environ['CLASSPATH']
        os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.jar:" + os.environ['SPARK_DIST_CLASSPATH']
        #os.environ['HADOOP_CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.jar:" + os.environ['HADOOP_CLASSPATH']

        pyhdfs_handle = pydoophdfs.hdfs(host='default', port=0, user=hopshdfs.project_user())

        #Create output directory for TensorBoard events for this executor
        #REMOVE THIS LATER!!!!!!!!!! Folder should be created automatically
        hdfs_events_parent_dir = hopshdfs.project_path() + "/Logs/Tensorboard"
        if not pyhdfs_handle.exists(hdfs_events_parent_dir):
            pyhdfs_handle.create_directory(hdfs_events_parent_dir)

        hdfs_appid_logdir = hdfs_events_parent_dir + "/" + app_id
        if not pyhdfs_handle.exists(hdfs_appid_logdir):
            pyhdfs_handle.create_directory(hdfs_appid_logdir)

        hdfs_run_id_logdir = hdfs_appid_logdir + "/" + "runId." + str(run_id)
        if not pyhdfs_handle.exists(hdfs_run_id_logdir):
            pyhdfs_handle.create_directory(hdfs_run_id_logdir)

        hdfs_exec_logdir = hdfs_run_id_logdir + "/executor." + str(executor_num)
        if not pyhdfs_handle.exists(hdfs_exec_logdir):
            pyhdfs_handle.create_directory(hdfs_exec_logdir)

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
                tb_hdfs_path, tb_pid = tensorboard.register(hdfs_exec_logdir, hdfs_appid_logdir, executor_num, param_string=param_string)
                map_fun(*args)
            else:
                tb_hdfs_path, tb_pid = tensorboard.register(hdfs_exec_logdir, hdfs_appid_logdir, executor_num)
                map_fun()

        except:
            #Always kill tensorboard
            if tb_pid != 0:
                cleanup(tb_pid, tb_hdfs_path)
            raise

        if tb_pid != 0:
            cleanup(tb_pid, tb_hdfs_path)

    return _wrapper_fun

def cleanup(handle, tb_pid, tb_hdfs_path):
    if tb_pid != 0:
        subprocess.Popen(["kill", str(tb_pid)])
        handle.delete(tb_hdfs_path, user=hopshdfs.project_user())
