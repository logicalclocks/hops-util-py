"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import os
from hopsutil import hdfs as hopshdfs
import pydoop.hdfs.fs as pydoophdfs
from hopsutil import tensorboard

def launch(spark_session, map_fun, args_dict=None):
    sc = spark_session.sparkContext
    app_id = str(sc.applicationId)

    if args_dict == None:
        num_executors = 1
    else:
        num_executors = args_dict.values()[0].len()

    #TF task should be run on 1 executor
    nodeRDD = sc.parallelize(range(num_executors), num_executors)

    #Force execution on executor, since GPU is located on executor
    nodeRDD.foreachPartition(prepare_func(app_id, map_fun, args_dict))

#Helper to put Spark required parameter iter in function signature
def prepare_func(app_id, map_fun, args_dict):

    def _wrapper_fun(iter):

        for i in iter:
            executor_num = i

        #Temporary crap fix
        os.environ['CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.jar:" + os.environ['CLASSPATH']
        os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.jar:" + os.environ['SPARK_DIST_CLASSPATH']

        pyhdfs_handle = pydoophdfs.hdfs(host='default', port=0, user=hopshdfs.project_user())

        #Start TensorBoard automatically
        tensorboard.register(app_id)

        #Arguments
        if args_dict:
            argcount = map_fun.func_code.co_argcount
            names = map_fun.func_code.co_varnames

            args = []
            argIndex = 0
            while argcount > 0:
                #Get args for executor and run function
                args.append(args_dict[names[argIndex]][iter])
                argcount -= 1
                argIndex += 1
            map_fun(*args)
        else:
            map_fun()

        #Create output directory for TensorBoard events for this executor
        hdfs_events_parent_dir = hopshdfs.project_path() + "/Jupyter/Tensorboard"
        if not pyhdfs_handle.exists(hdfs_events_parent_dir):
            pyhdfs_handle.create_directory(hdfs_events_parent_dir)
        hdfs_events_logdir = hdfs_events_parent_dir + "/" + app_id + ".exec." + executor_num
        pyhdfs_handle.create_directory(hdfs_events_logdir)

        #Write TensorBoard logdir contents to HDFS
        executor_events_dir = tensorboard.get_logdir()
        for filename in os.listdir(executor_events_dir):
            pyhdfs_handle.copy(os.path.join(executor_events_dir, filename), pyhdfs_handle, hdfs_events_logdir)

    return _wrapper_fun