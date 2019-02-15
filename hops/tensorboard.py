"""

Utility functions to manage the lifecycle of TensorBoard and get the path to write TensorBoard events.

"""
import socket
import subprocess
import time
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
local_logdir_path = None
local_logdir_bool = False

def _register(hdfs_exec_dir, endpoint_dir, exec_num, local_logdir=False):
    """

    Args:
        hdfs_exec_dir:
        endpoint_dir:
        exec_num:
        local_logdir:

    Returns:

    """
    global tb_pid

    if tb_pid != 0:
        subprocess.Popen(["kill", str(tb_pid)])

    _reset_global()

    global events_logdir
    events_logdir = hdfs_exec_dir

    global local_logdir_bool
    local_logdir_bool = local_logdir


    if tb_pid == 0:
        global pypath
        pypath = os.getenv("PYSPARK_PYTHON")

        #find free port
        tb_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tb_socket.bind(('',0))
        global tb_port
        tb_addr, tb_port = tb_socket.getsockname()

        global tb_path
        tb_path = util._find_tensorboard()

        tb_socket.close()

        tb_env = os.environ.copy()
        tb_env['CUDA_VISIBLE_DEVICES'] = ''

        tb_proc = None
        global local_logdir_path
        if local_logdir:
            local_logdir_path = os.getcwd() + '/local_logdir'
            if os.path.exists(local_logdir_path):
                shutil.rmtree(local_logdir_path)
                os.makedirs(local_logdir_path)
            else:
                os.makedirs(local_logdir_path)

            local_logdir_path = local_logdir_path + '/'
            tb_proc = subprocess.Popen([pypath, tb_path, "--logdir=%s" % local_logdir_path, "--port=%d" % tb_port, "--host=%s" % "0.0.0.0"],
                                       env=tb_env, preexec_fn=util._on_executor_exit('SIGTERM'))
        else:
            tb_proc = subprocess.Popen([pypath, tb_path, "--logdir=%s" % events_logdir, "--port=%d" % tb_port, "--host=%s" % "0.0.0.0"],
                                   env=tb_env, preexec_fn=util._on_executor_exit('SIGTERM'))

        tb_pid = tb_proc.pid

        host = socket.gethostname()
        global tb_url
        tb_url = "http://{0}:{1}".format(host, tb_port)
        global endpoint
        endpoint = endpoint_dir + "/TensorBoard.task" + str(exec_num)

        #dump tb host:port to hdfs
    pydoop.hdfs.dump(tb_url, endpoint, user=hopshdfs.project_user())

    return endpoint, tb_pid

def logdir():
    """
    Get the TensorBoard logdir. This function should be called in your wrapper function for Experiment, Parallel Experiment or Distributed Training and passed as the
    logdir for TensorBoard.

    *Case 1: local_logdir=True*, then the logdir is on the local filesystem, otherwise it is in the folder for your experiment in your project in HDFS. Once the experiment is finished all the files that are present in the directory will be uploaded to tour experiment directory in the Experiments dataset.

    *Case 2: local_logdir=False*, then the logdir is in HDFS in your experiment directory in the Experiments dataset.

    Returns:
        The path to store files for your experiment. The content is also visualized in TensorBoard.
    """

    global local_logdir_bool
    if local_logdir_bool:
        return local_logdir_path

    global events_logdir
    return events_logdir

def interactive_debugger():
    """

    Returns: address for interactive debugger in TensorBoard


    """
    global debugger_endpoint
    debugger_endpoint =_restart_debugging()
    return debugger_endpoint

def non_interactive_debugger():
    """

    Returns: address for non-interactive debugger in TensorBoard

    """
    global debugger_endpoint
    debugger_endpoint =_restart_debugging(interactive=False)
    return debugger_endpoint

def _restart_debugging(interactive=True):
    """

    Args:
        interactive:

    Returns:

    """
    global tb_pid

    #Kill existing TB
    proc = subprocess.Popen(["kill", str(tb_pid)])
    proc.wait()

    debugger_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    debugger_socket.bind(('',0))
    debugger_addr, debugger_port = debugger_socket.getsockname()

    debugger_socket.close()

    tb_env = os.environ.copy()
    tb_env['CUDA_VISIBLE_DEVICES'] = ''

    global pypath
    global tb_path
    global tb_port

    if interactive:
        tb_proc = subprocess.Popen([pypath, tb_path, "--logdir=%s" % logdir(), "--port=%d" % tb_port, "--debugger_port=%d" % debugger_port, "--host=%s" % "0.0.0.0"],
                                   env=tb_env, preexec_fn=util._on_executor_exit('SIGTERM'))
        tb_pid = tb_proc.pid

    if not interactive:
        tb_proc = subprocess.Popen([pypath, tb_path, "--logdir=%s" % logdir(), "--port=%d" % tb_port, "--debugger_data_server_grpc_port=%d" % debugger_port, "--host=%s" % "0.0.0.0"],
                                   env=tb_env, preexec_fn=util._on_executor_exit('SIGTERM'))
        tb_pid = tb_proc.pid

    time.sleep(2)

    return 'localhost:' + str(debugger_port)


def visualize(hdfs_root_logdir):
    """ Visualize all TensorBoard events for a given path in HopsFS. This is intended for use after running TensorFlow jobs to visualize
    them all in the same TensorBoard. tflauncher.launch returns the path in HopsFS which should be handed as argument for this method to visualize all runs.

    Args:
      :hdfs_root_logdir: the path in HopsFS to enter as the logdir for TensorBoard
    """

    sc = util._find_spark().sparkContext
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

    tb_path = util._find_tensorboard()

    tb_socket.close()

    tb_env = os.environ.copy()
    tb_env['CUDA_VISIBLE_DEVICES'] = ''

    tb_proc = subprocess.Popen([pypath, tb_path, "--logdir=%s" % logdir, "--port=%d" % tb_port, "--host=%s" % "0.0.0.0"],
                               env=tb_env, preexec_fn=util._on_executor_exit('SIGTERM'))

    host = socket.gethostname()
    tb_url = "http://{0}:{1}".format(host, tb_port)
    tb_endpoint = hopshdfs._get_experiments_dir() + "/" + app_id + "/TensorBoard.visualize"
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

def _reset_global():
    """

    Returns:

    """
    global root_logdir_path
    global events_logdir
    global tb_pid
    global tb_url
    global tb_port
    global endpoint
    global debugger_endpoint
    global pypath
    global tb_path
    global local_logdir_path
    global local_logdir_bool
    
    root_logdir_path = None
    events_logdir = None
    tb_pid = 0
    tb_url = None
    tb_port = None
    endpoint = None
    debugger_endpoint = None
    pypath = None
    tb_path = None
    local_logdir_path = None
    local_logdir_bool = False
