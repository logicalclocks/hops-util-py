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
endpoint = None

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
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('',0))
        addr, port = s.getsockname()
        s.close()
        tb_path = util.find_tensorboard()
        tb_proc = subprocess.Popen([pypath, tb_path, "--logdir=%s" % events_logdir, "--port=%d" % port],
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

def logdir():
    """ Get the TensorBoard logdir. This function should be called in your code for TensorFlow, TensorFlowOnSpark or Horovod and passed as the
    logdir for TensorBoard. Any files written to this directory will be put in your HopsWorks project Logs dataset, so writing the model to this folder could be an alternative
     solution to writing it directly to HopsFS

    Returns:
      The local directory to write TensorBoard events and summaries to
    """
    global events_logdir
    return events_logdir

def visualize(spark_session, hdfs_root_logdir):
    """ Visualize all TensorBoard events for a given path in HopsFS. This is intended for use after running TensorFlow jobs to visualize
    them all in the same TensorBoard. tflauncher.launch returns the path in HopsFS which should be handed as argument for this method to visualize all runs.

    Args:
      :spark_session: SparkSession object
      :hdfs_root_logdir: the path in HopsFS to enter as the logdir for TensorBoard
    """

    sc = spark_session.sparkContext
    app_id = str(sc.applicationId)

    num_executions = 1
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