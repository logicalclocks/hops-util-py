"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import threading

def launch(sc, map_fun):
    nodeRDD = sc.parallelize(range(1), 1)

    # start TF on a background thread (on Spark driver) to allow for feeding job
    def _start():
        nodeRDD.foreachPartition(map_fun)

    t = threading.Thread(target=_start)
    t.start()
