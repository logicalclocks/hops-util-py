"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

def launch(sc, map_fun):

    #TF task should be run on 1 executor
    nodeRDD = sc.parallelize(range(1), 1)

    #Force execution on executor (driver doesn't have GPU)
    nodeRDD.foreachPartition(map_fun)
