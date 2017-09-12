"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import os
from hopsutil import hdfs
from hopsutil import tensorboard

def launch(sc, map_fun, args_dict):

    if args_dict == None:
        num_executors = 1
    else:
        num_executors = args_dict.values()[0].len()

    #TF task should be run on 1 executor
    nodeRDD = sc.parallelize(range(num_executors), num_executors)

    #Force execution on executor, since GPU is located on executor
    nodeRDD.foreachPartition(prepare_func(map_fun, args_dict))


#Helper to put Spark required parameter iter in function signature
def prepare_func(map_fun, args_dict):

    def _wrapper_fun(iter):

        #Arguments
        if args_dict != None:
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
            raise ValueError('You forgot to define args_dict with arguments for TensorFlow function')

        #Start TensorBoard automatically
        tensorboard.register()

        #Current applicationId
        app_id = os.getenv("APP_ID")

        #Create output directory for TensorBoard events for this executor
        hdfs_events_parent_dir = hdfs.project_path() + "/Jupyter/Tensorboard"
        dir_exists = hdfs.exists(hdfs_events_parent_dir)
        if dir_exists == False:
            hdfs.create_directory(hdfs_events_parent_dir)
        hdfs_events_logdir = hdfs_events_parent_dir + "/" + app_id + ".exec." + iter
        hdfs.create_directory(hdfs_events_logdir)

        #Write TensorBoard logdir contents to HDFS
        executor_events_dir = tensorboard.get_logdir()
        for filename in os.listdir(executor_events_dir):
            hdfs.copy(os.path.join(executor_events_dir, filename), hdfs, hdfs_events_logdir)

    return _wrapper_fun