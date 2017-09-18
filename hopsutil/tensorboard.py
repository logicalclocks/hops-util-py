"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import socket
import subprocess
import os
from hopsutil import hdfs as hopshdfs
import pydoop.hdfs

logdir = ''

def register(hdfs_exec_dir, exec_id):

    global logdir
    logdir = hdfs_exec_dir

    if not os.path.exists(logdir):
        os.makedirs(logdir)

    pypath = os.getenv("PYSPARK_PYTHON")
    pydir = os.path.dirname(pypath)

    #find free port
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('',0))
    addr, port = s.getsockname()
    s.close()

    tb_proc = subprocess.Popen([pypath, "%s/tensorboard"%pydir, "--logdir=%s"%logdir, "--port=%d"%port, "--debug"])
    tb_pid = tb_proc.pid

    host = socket.gethostname()
    tb_url = "http://{0}:{1}".format(host, port)

    #dump tb host:port to hdfs
    pydoop.hdfs.dump(tb_url, logdir + "/tensorboard.endpoint" + exec_id, user=hopshdfs.project_user())

    return tb_pid

def logdir():
    return logdir
