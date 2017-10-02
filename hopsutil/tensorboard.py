"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import socket
import subprocess
import os
from hopsutil import hdfs as hopshdfs
import pydoop.hdfs

logdir_path = None
events_logdir = None

def register(hdfs_exec_dir, endpoint_dir, exec_num, param_string=None):

    global events_logdir
    events_logdir = hdfs_exec_dir

    global logdir_path
    logdir_path = os.getcwd() + '/tensorboard_events'

    if not os.path.exists(logdir_path):
        os.makedirs(logdir_path)

    pypath = os.getenv("PYSPARK_PYTHON")
    pydir = os.path.dirname(pypath)

    #find free port
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('',0))
    addr, port = s.getsockname()
    s.close()

    tb_proc = subprocess.Popen([pypath, "%s/tensorboard"%pydir, "--logdir=%s"%logdir_path, "--port=%d"%port, "--debug"])
    tb_pid = tb_proc.pid

    host = socket.gethostname()
    tb_url = "http://{0}:{1}".format(host, port)

    if param_string:
        path = endpoint_dir + "/tensorboard.exec" + str(exec_num) + "." + param_string
    else:
        path = endpoint_dir + "/tensorboard.exec" + str(exec_num)

    #dump tb host:port to hdfs
    pydoop.hdfs.dump(tb_url, path, user=hopshdfs.project_user())

    return path, tb_pid

def store():
    pydoop.hdfs.put(logdir(), events_logdir)

def logdir():
    return logdir_path
