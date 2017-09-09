"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""
import os
from hopsutil import hdfs
from hopsutil import tensorboard

def launch(sc, map_fun):

    #TF task should be run on 1 executor
    nodeRDD = sc.parallelize(range(1), 1)

    #Force execution on executor, since GPU is located on executor
    nodeRDD.foreachPartition(prepare_func(map_fun))

#Helper to put Spark required parameter iter in function signature
def prepare_func(map_fun):
    #TODO: Consume iter and put in list of hyperparams
    def _wrapper_fun(iter):

        #Start TensorBoard automatically
        tensorboard.register()

        #TensorFlow script
        map_fun()

        #Are we allowed to create this?
        hdfs_events_dir = hdfs.project_path() + "/Jupyter/Tensorboard"
        hdfs.create_directory(hdfs_events_dir)

        #Write TensorBoard logdir contents to HDFS
        executor_events_dir = tensorboard.get_logdir()
        for filename in os.listdir(executor_events_dir):
            hdfs.copy(os.path.join(executor_events_dir, filename), hdfs, hdfs_events_dir)

    return _wrapper_fun