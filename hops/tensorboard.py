""" This module is used to manage the life-cycle of the TensorBoard """

import socket
import subprocess
import os
from hops import hdfs as hopshdfs
from hops import util
import pydoop.hdfs
import shutil
from os.path import splitext

root_logdir_path = None
events_logdir = None
tb_pid = 0
tb_url = None
tb_port = None
endpoint = None
debugger_endpoint = None
pypath = None
tb_path = None

def register(hdfs_exec_dir, endpoint_dir, exec_num):

    global events_logdir
    events_logdir = hdfs_exec_dir

    global tb_pid

    if tb_pid != 0:
        subprocess.Popen(["kill", str(tb_pid)])

    tb_pid = 0

    if tb_pid == 0:
        pypath = os.getenv("PYSPARK_PYTHON")

        #find free port
        tb_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tb_socket.bind(('',0))
        global tb_port
        tb_addr, tb_port = tb_socket.getsockname()

        debugger_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        debugger_socket.bind(('',0))
        debugger_addr, debugger_port = debugger_socket.getsockname()

        tb_path = util.find_tensorboard()

        tb_socket.close()

        tb_proc = subprocess.Popen([pypath, tb_path, "--logdir=%s" % events_logdir, "--port=%d" % tb_port],
                                   env=os.environ, preexec_fn=util.on_executor_exit('SIGTERM'))
        tb_pid = tb_proc.pid

        global debugger_endpoint
        debugger_endpoint = 'localhost:' + str(debugger_port)

        host = socket.gethostname()
        global tb_url
        tb_url = "http://{0}:{1}".format(host, tb_port)
        global endpoint
        endpoint = endpoint_dir + "/tensorboard.exec" + str(exec_num)

        #dump tb host:port to hdfs
    pydoop.hdfs.dump(tb_url, endpoint, user=hopshdfs.project_user())

    return endpoint, tb_pid

def logdir():
    """ Get the TensorBoard logdir. This function should be called in your code for TensorFlow, TensorFlowOnSpark or Horovod and passed as the
    logdir for TensorBoard. Any files written to this directory will be put in your HopsWorks project Logs dataset, so writing the model to this folder could be an alternative
     solution to writing it directly to HopsFS

    Returns:
      The local directory to write TensorBoard events and summaries to
    """
    if 'TENSORBOARD_LOGDIR' in os.environ:
        return os.environ['TENSORBOARD_LOGDIR']

    global events_logdir
    return events_logdir

def interactive_debugger():

    global debugger_endpoint
    debugger_endpoint =_restart_debugging()
    return debugger_endpoint

def non_interactive_debugger():

    global debugger_endpoint
    debugger_endpoint =_restart_debugging(interactive=False)
    return debugger_endpoint

def _restart_debugging(interactive=True):

    #Kill existing TB

    proc = subprocess.Popen(["kill", str(tb_pid)])
    proc.wait()

    debugger_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    debugger_socket.bind(('',0))
    debugger_addr, debugger_port = debugger_socket.getsockname()

    debugger_socket.close()

    global pypath
    global tb_path
    global tb_port
    global tb_pid

    if interactive:
        tb_proc = subprocess.Popen([pypath, tb_path, "--logdir=%s" % logdir(), "--port=%d" % tb_port, "--debugger_port=%d" % debugger_port],
                                   env=os.environ, preexec_fn=util.on_executor_exit('SIGTERM'))
        tb_pid = tb_proc.pid

    if not interactive:
        tb_proc = subprocess.Popen([pypath, tb_path, "--logdir=%s" % logdir(), "--port=%d" % tb_port, "--debugger_data_server_grpc_port=%d" % debugger_port],
                                   env=os.environ, preexec_fn=util.on_executor_exit('SIGTERM'))
        tb_pid = tb_proc.pid

    return 'localhost:' + str(debugger_port)


def visualize(spark_session, hdfs_root_logdir):
    """ Visualize all TensorBoard events for a given path in HopsFS. This is intended for use after running TensorFlow jobs to visualize
    them all in the same TensorBoard. tflauncher.launch returns the path in HopsFS which should be handed as argument for this method to visualize all runs.

    Args:
      :spark_session: SparkSession object
      :hdfs_root_logdir: the path in HopsFS to enter as the logdir for TensorBoard
    """

    sc = spark_session.sparkContext
    app_id = str(sc.applicationId)

    pypath = os.getenv("PYSPARK_PYTHON")

    logdir = os.getcwd() + '/tensorboard_events/'
    if os.path.exists(logdir):
       shutil.rmtree(logdir)
       os.makedirs(logdir)
    else:
       os.makedirs(logdir)

       #find free port
    tb_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tb_socket.bind(('',0))
    tb_addr, tb_port = tb_socket.getsockname()

    tb_path = util.find_tensorboard()

    tb_socket.close()

    tb_proc = subprocess.Popen([pypath, tb_path, "--logdir=%s" % logdir, "--port=%d" % tb_port],
                               env=os.environ, preexec_fn=util.on_executor_exit('SIGTERM'))

    host = socket.gethostname()
    tb_url = "http://{0}:{1}".format(host, tb_port)
    tb_endpoint = hopshdfs.project_path() + "/Logs/TensorFlow/" + app_id + "/tensorboard.exec0"
    #dump tb host:port to hdfs
    pydoop.hdfs.dump(tb_url, tb_endpoint, user=hopshdfs.project_user())

    handle = hopshdfs.get()
    hdfs_logdir_entries = handle.list_directory(hdfs_root_logdir)
    for entry in hdfs_logdir_entries:
        file_name, extension = splitext(entry['name'])
        if not extension == '.log':
            pydoop.hdfs.get(entry['name'], logdir)

    tb_proc.wait()
    stdout, stderr = tb_proc.communicate()
    print(stdout)
    print(stderr)