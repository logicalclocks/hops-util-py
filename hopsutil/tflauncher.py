"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""
import os
from hopsutil import hdfs
from hopsutil import tensorboard

hyperparams = None

def launch(sc, map_fun):

    if hyperparams != None:
        num_executors = hyperparams.values()[0].len()

    #TF task should be run on 1 executor
    nodeRDD = sc.parallelize(range(num_executors), num_executors)

    #Force execution on executor, since GPU is located on executor
    nodeRDD.foreachPartition(prepare_func(map_fun))

def set_hyperparams(grid):
    global hyperparams
    hyperparams = grid


#Helper to put Spark required parameter iter in function signature
def prepare_func(map_fun):
    #TODO: Consume iter and put in list of hyperparams
    def _wrapper_fun(iter):

        #Start TensorBoard automatically
        tensorboard.register()

        #TensorFlow script
        if hyperparams == None:
            map_fun()
        else:
            argcount = map_fun.func_code.co_argcount
            names = map_fun.func_code.co_varnames

            args = []
            argIndex = 0
            while argcount > 0:
                #Get hyperparam value for
                args.append(hyperparams[names[argIndex]][iter])
                argcount -= 1
                argIndex += 1
            map_fun(*args)

        #Current applicationId
        app_id = os.getenv("APP_ID")

        #Are we allowed to create this?

        hdfs_events_parent_dir = hdfs.project_path() + "/Jupyter/Tensorboard"
        dir_exists = hdfs.exists(hdfs_events_parent_dir)
        if dir_exists == False:
            hdfs.create_directory(hdfs_events_parent_dir)
        hdfs_events_logdir = hdfs_events_parent_dir + "/" + app_id
        hdfs.create_directory(hdfs_events_logdir)

        #Write TensorBoard logdir contents to HDFS
        executor_events_dir = tensorboard.get_logdir()
        for filename in os.listdir(executor_events_dir):
            hdfs.copy(os.path.join(executor_events_dir, filename), hdfs, hdfs_events_logdir)

    return _wrapper_fun