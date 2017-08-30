"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

def launch(sc, map_fun):

    #TODO method signature should be manipulated and added an argument to it (iter)

    #TF task should be run on 1 executor
    nodeRDD = sc.parallelize(range(1), 1)

    #Force execution on executor, since GPU is located on executor
    nodeRDD.foreachPartition(prepare_func(map_fun))

#Helper to put Spark required parameter iter in function signature
def prepare_func(map_fun):
    def _wrapper_fun(iter):
        map_fun()
    return _wrapper_fun