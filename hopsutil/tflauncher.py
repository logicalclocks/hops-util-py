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

def launch(spark_session, map_fun, args_dict=None):

    #Temporary crap fix
    os.environ['CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.jar:" + os.environ['CLASSPATH']
    os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.jar:" + os.environ['SPARK_DIST_CLASSPATH']
    #os.environ['HADOOP_CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.jar:" + os.environ['HADOOP_CLASSPATH']

    sc = spark_session.sparkContext
    app_id = str(sc.applicationId)

    time = datetime.datetime.now()
    timestamp = "%s.%s.%s" % (time.hour, time.minute, time.second)

    if args_dict == None:
        num_executors = 1
    else:
        num_executors = len(args_dict.values()[0])

    #TF task should be run on 1 executor
    nodeRDD = sc.parallelize(range(num_executors), num_executors)

    #Force execution on executor, since GPU is located on executor
    nodeRDD.foreachPartition(prepare_func(app_id, timestamp, map_fun, args_dict))

#Helper to put Spark required parameter iter in function signature
def prepare_func(app_id, timestamp, map_fun, args_dict):

    def _wrapper_fun(iter):
        print "test1"
        for i in iter:
            executor_num = i
        print "test2"
        #Temporary crap fix
        os.environ['CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.jar:" + os.environ['CLASSPATH']
        os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.jar:" + os.environ['SPARK_DIST_CLASSPATH']
        #os.environ['HADOOP_CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.jar:" + os.environ['HADOOP_CLASSPATH']

        pyhdfs_handle = pydoophdfs.hdfs(host='default', port=0, user=hopshdfs.project_user())
        print "test3"
        #Create output directory for TensorBoard events for this executor
        #REMOVE THIS LATER!!!!!!!!!! Folder should be created automatically
        hdfs_events_parent_dir = hopshdfs.project_path() + "/Logs/Tensorboard"
        if not pyhdfs_handle.exists(hdfs_events_parent_dir):
            pyhdfs_handle.create_directory(hdfs_events_parent_dir)
        print "test4"
        hdfs_appid_logdir = hdfs_events_parent_dir + "/" + app_id
        if not pyhdfs_handle.exists(hdfs_appid_logdir):
            pyhdfs_handle.create_directory(hdfs_appid_logdir)
        print "test5"
        hdfs_timestamp_logdir = hdfs_appid_logdir + "/" + timestamp
        if not pyhdfs_handle.exists(hdfs_timestamp_logdir):
            pyhdfs_handle.create_directory(hdfs_timestamp_logdir)
        print "test6"
        hdfs_exec_logdir = hdfs_timestamp_logdir + "/executor." + str(executor_num)
        if not pyhdfs_handle.exists(hdfs_exec_logdir):
            pyhdfs_handle.create_directory(hdfs_exec_logdir)
        print "test7"
        #Start TensorBoard automatically
        tb_pid = tensorboard.register(hdfs_exec_logdir)
        print "test8"
        try:
            #Arguments
            if args_dict:
                argcount = map_fun.func_code.co_argcount
                names = map_fun.func_code.co_varnames

                args = []
                argIndex = 0
                while argcount > 0:
                    #Get args for executor and run function
                    print "test9"
                    args.append(args_dict[names[argIndex]][executor_num])
                    argcount -= 1
                    argIndex += 1
                print "test10"
                map_fun(*args)
            else:
                map_fun()
                print "test11"
        except:
            #Always kill tensorboard
            if tb_pid != 0:
                subprocess.Popen(["kill", str(tb_pid)])
            raise

        if tb_pid != 0:
            subprocess.Popen(["kill", str(tb_pid)])

    return _wrapper_fun