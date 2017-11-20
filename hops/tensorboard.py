"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

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
exec_logdir = None
params = None
dir_counter = 0
tb_pid = 0
tb_url = None
endpoint = None

def register(hdfs_exec_dir, endpoint_dir, exec_num, param_string=None):

    global events_logdir
    events_logdir = hdfs_exec_dir

    global root_logdir_path
    root_logdir_path = root_logdir()

    global params
    params = param_string

    if not os.path.exists(root_logdir_path):
        os.makedirs(root_logdir_path)

    global exec_logdir
    if params:
        exec_logdir = root_logdir_path + '/' + params
    else:
        exec_logdir = root_logdir_path

    if not os.path.exists(exec_logdir):
        os.makedirs(exec_logdir)
    else:
        shutil.rmtree(exec_logdir)
        os.makedirs(exec_logdir)

    global tb_pid
    if tb_pid == 0:
        pypath = os.getenv("PYSPARK_PYTHON")

        #find free port
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('',0))
        addr, port = s.getsockname()
        s.close()
        tb_path = util.find_tensorboard()
        tb_proc = subprocess.Popen([pypath, tb_path, "--logdir=%s" % root_logdir_path, "--port=%d" % port],
                                   env=os.environ, preexec_fn=util.on_executor_exit('SIGTERM'))
        tb_pid = tb_proc.pid

        host = socket.gethostname()
        global tb_url
        tb_url = "http://{0}:{1}".format(host, port)
        global endpoint
        endpoint = endpoint_dir + "/tensorboard.exec" + str(exec_num)

        #dump tb host:port to hdfs
    pydoop.hdfs.dump(tb_url, endpoint, user=hopshdfs.project_user())

    return endpoint, tb_pid

def store():
    handle = hopshdfs.get()
    if not events_logdir == None:
        handle.delete(events_logdir, recursive=True)
        hopshdfs.log('Storing ' + exec_logdir + ' in ' + events_logdir)
        pydoop.hdfs.put(exec_logdir, events_logdir)

def root_logdir():
    root_logdir_path = os.getcwd() + '/tensorboard_events'
    return root_logdir_path

def logdir():
    if "TENSORBOARD_LOGDIR" in os.environ:
        return os.environ['TENSORBOARD_LOGDIR']
    return exec_logdir

def clean():
    #shutil.rmtree(exec_logdir)
    global params, exec_logdir, events_logdir
    exec_logdir = None
    events_logdir = None
    params = None
    global dir_counter
    dir_counter += 1

def visualize(spark_session, hdfs_root_logdir):

    sc = spark_session.sparkContext
    app_id = str(sc.applicationId)

    num_executions = 1#Each TF task should be run on 1 executor
    nodeRDD = sc.parallelize(range(num_executions), num_executions)
    nodeRDD.foreachPartition(start_visualization(hdfs_root_logdir, app_id))

def start_visualization(hdfs_root_logdir, app_id):

    def _wrapper_fun(iter):

        os.environ['CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['CLASSPATH']
        os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['SPARK_DIST_CLASSPATH']
        os.environ['CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['CLASSPATH']
        os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['SPARK_DIST_CLASSPATH']
        os.environ['CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.2.jar:" + os.environ['CLASSPATH']
        os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.2.jar:" + os.environ['SPARK_DIST_CLASSPATH']
        os.environ['CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.2.jar:" + os.environ['CLASSPATH']
        os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.2.jar:" + os.environ['SPARK_DIST_CLASSPATH']

        for i in iter:
            executor_num = i
            pypath = os.getenv("PYSPARK_PYTHON")

        logdir = os.getcwd() + '/tensorboard_events/'
        if os.path.exists(logdir):
            shutil.rmtree(logdir)
            os.makedirs(logdir)


        handle = hopshdfs.get()
        hdfs_logdir_entries = handle.list_directory(hdfs_root_logdir)
        for entry in hdfs_logdir_entries:
            file_name, extension = splitext(entry['name'])
            if not extension == '.log':
                pydoop.hdfs.get(entry['name'], logdir)

        #find free port
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('',0))
        addr, port = s.getsockname()
        s.close()
        tb_path = util.find_tensorboard()
        tb_proc = subprocess.Popen([pypath, tb_path, "--logdir=%s" % logdir, "--port=%d" % port],
                                   env=os.environ, preexec_fn=util.on_executor_exit('SIGTERM'))

        host = socket.gethostname()
        tb_url = "http://{0}:{1}".format(host, port)

        tb_endpoint = hopshdfs.project_path() + "/Logs/TensorFlow/" + app_id + "/tensorboard.exec" + str(executor_num)

        #dump tb host:port to hdfs
        pydoop.hdfs.dump(tb_url, tb_endpoint, user=hopshdfs.project_user())

        tb_proc.wait()
        stdout, stderr = tb_proc.communicate()
        print(stdout)
        print(stderr)

    return _wrapper_fun