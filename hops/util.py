"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import os
import sys
import signal
from ctypes import cdll
from hops import hdfs as hopshdfs

def _find_in_path(path, file):
    """Find a file in a given path string."""
    for p in path.split(os.pathsep):
        candidate = os.path.join(p, file)
        if (os.path.exists(os.path.join(p, file))):
            return candidate
    return False

def find_tensorboard():
    pypath = os.getenv("PYSPARK_PYTHON")
    pydir = os.path.dirname(pypath)
    search_path = os.pathsep.join([pydir, os.environ['PATH'], os.environ['PYTHONPATH']])
    tb_path = _find_in_path(search_path, 'tensorboard')
    if not tb_path:
        raise Exception("Unable to find 'tensorboard' in: {}".format(search_path))
    return tb_path

def on_executor_exit(signame, endpoint):
    """
    Return a function to be run in a child process which will trigger
    SIGNAME to be sent when the parent process dies
    """
    signum = getattr(signal, signame)
    def set_parent_exit_signal():
        # http://linux.die.net/man/2/prctl

        handle = hopshdfs.get()
        handle.delete(endpoint)

        PR_SET_PDEATHSIG = 1
        result = cdll['libc.so.6'].prctl(PR_SET_PDEATHSIG, signum)
        if result != 0:
            raise Exception('prctl failed with error code %s' % result)
    return set_parent_exit_signal

def num_executors():
    from pyspark.context import SparkContext
    from pyspark.conf import SparkConf
    sc = spark.sparkContext
    return int(sc._conf.get("spark.executor.instances"))

def num_param_servers():
    from pyspark.context import SparkContext
    from pyspark.conf import SparkConf
    sc = spark.sparkContext
    return int(sc._conf.get("spark.tensorflow.num.ps"))
