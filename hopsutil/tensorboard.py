"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import socket
import subprocess
import os
import hdfs

def register(logdir):

    #find free port
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('',0))
    addr, port = s.getsockname()
    s.close()

    #let tb bind to port
    subprocess.Popen([os.getenv("PYSPARK_PYTHON"), "tensorboard", "--logdir=%s"%logdir, "--port=%d"%port, "--debug"])
    tb_url = "http://{0}:{1}".format(addr, port)

    #dump tb host:port to hdfs
    hops_user = os.environ["USER"];
    hops_user_split = hops_user.split("__");
    project = hops_user_split[0];
    hdfs_handle = hdfs.get()
    hdfs_handle.dump(tb_url, "hdfs:///Projects/" + project + "/Resources/.jupyter.tensorboard", user=hops_user)



