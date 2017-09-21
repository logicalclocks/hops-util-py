"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import socket
import subprocess
import os
from hopsutil import hdfs as hopshdfs
import pydoop.hdfs

logdir_path = ''

def register(hdfs_exec_dir, exec_num, param_string=None):

    global logdir_path
    logdir_path = hdfs_exec_dir

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
        path = logdir_path + "tensorboard.exec" + str(exec_num) + "." + param_string
    else:
        path = logdir_path + "tensorboard.exec" + str(exec_num)

    #dump tb host:port to hdfs
    pydoop.hdfs.dump(tb_url, path, user=hopshdfs.project_user())

    return tb_pid

def logdir():
    return logdir_path
