"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import pydoop.hdfs
import subprocess
import os
import stat
import sys
import threading

from hops import hdfs as hopshdfs
from hops import tensorboard
from hops import devices
from hops import util

run_id = 0

def launch(spark_session, notebook):
    """ Run notebook pointed to in HopsFS as a python file in mpirun

    Args:
      :spark_session: SparkSession object
      :notebook: The path in HopsFS to the notebook
    """

    print('\nStarting TensorFlow job, follow your progress on TensorBoard in Jupyter UI! \n')
    sys.stdout.flush()

    #Temporary crap fix
    os.environ['CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['CLASSPATH']
    os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['SPARK_DIST_CLASSPATH']
    os.environ['CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['CLASSPATH']
    os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['SPARK_DIST_CLASSPATH']
    os.environ['CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.2.jar:" + os.environ['CLASSPATH']
    os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.2.jar:" + os.environ['SPARK_DIST_CLASSPATH']
    os.environ['CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.2.jar:" + os.environ['CLASSPATH']
    os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.2.jar:" + os.environ['SPARK_DIST_CLASSPATH']

    sc = spark_session.sparkContext
    app_id = str(sc.applicationId)

    conf_num = int(sc._conf.get("spark.executor.instances"))

    #Each TF task should be run on 1 executor
    nodeRDD = sc.parallelize(range(conf_num), conf_num)

    #Force execution on executor, since GPU is located on executor
    nodeRDD.foreachPartition(prepare_func(app_id, run_id, notebook))

    print('Finished TensorFlow job \n')
    print('Make sure to check /Logs/TensorFlow/' + app_id + '/runId.' + str(run_id) + ' for logfile and TensorBoard logdir')

    global run_id
    run_id += 1

def prepare_func(app_id, run_id, nb_path):

    def _wrapper_fun(iter):

        for i in iter:
            executor_num = i

        #Temporary crap fix
        os.environ['CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['CLASSPATH']
        os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['SPARK_DIST_CLASSPATH']
        os.environ['CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['CLASSPATH']
        os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['SPARK_DIST_CLASSPATH']
        os.environ['CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.2.jar:" + os.environ['CLASSPATH']
        os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.2.jar:" + os.environ['SPARK_DIST_CLASSPATH']
        os.environ['CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.2.jar:" + os.environ['CLASSPATH']
        os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.2.jar:" + os.environ['SPARK_DIST_CLASSPATH']

        hdfs_exec_logdir, hdfs_appid_logdir = hopshdfs.create_directories(app_id, run_id, 0)

        tb_pid = 0
        tb_hdfs_path = ''

        pydoop.hdfs.dump('', os.environ['EXEC_LOGFILE'], user=hopshdfs.project_user())
        hopshdfs.init_logger()
        hopshdfs.log('Starting Spark executor with arguments')
        if executor_num == 0:
            tb_hdfs_path, tb_pid = tensorboard.register(hdfs_exec_logdir, hdfs_appid_logdir, 0)

        gpu_str = '\n\nChecking for GPUs in the environment\n' + devices.get_gpu_info()
        hopshdfs.log(gpu_str)
        print(gpu_str)

        #1. Download notebook file
        fs_handle = hopshdfs.get_fs()
        fd = fs_handle.open_file(nb_path, flags='r')
        notebook = ''
        for line in fd:
            notebook += line

        path, filename = os.path.split(nb_path)
        f_nb = open(filename,"w+")
        f_nb.write(notebook)
        f_nb.flush()
        f_nb.close()

        # 2. Convert notebook to py file
        jupyter_runnable = os.path.abspath(os.path.join(os.environ['PYSPARK_PYTHON'], os.pardir)) + '/jupyter'
        conversion_cmd = jupyter_runnable + ' nbconvert --to python ' + filename
        conversion = subprocess.Popen(conversion_cmd,
                           shell=True,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
        conversion.wait()
        stdout, stderr = conversion.communicate()
        print(stdout)
        print(stderr)

        # 3. Make py file runnable
        py_runnable = os.getcwd() + '/' + filename.split('.')[0] + '.py'
        st = os.stat(py_runnable)
        os.chmod(py_runnable, st.st_mode | stat.S_IEXEC)

        # 4. Run allreduce
        #mpi_np = os.environ['MPI_NP']
        mpi_cmd = 'HOROVOD_TIMELINE=' + tensorboard.logdir() + '/timeline.json' + \
                  ' TENSORBOARD_LOGDIR=' + tensorboard.logdir() + \
                  ' mpirun -np ' + str(devices.get_num_gpus()) + \
                  ' -x HOROVOD_TIMELINE ' + \
                  ' -x TENSORBOARD_LOGDIR ' + \
                  os.environ['PYSPARK_PYTHON'] + ' ' + py_runnable
        mpi = subprocess.Popen(mpi_cmd,
                       shell=True,
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE,
                       preexec_fn=util.on_executor_exit('SIGTERM'),
                       bufsize=1)

        mpi.wait()
        stdout, stderr = mpi.communicate()




        #stdout, stderr = mpi.communicate()
        return_code = mpi.returncode

        if return_code != 0:
            raise Exception('mpirun FAILED with the following outputs:' +
                      '\n\n STDOUT: ' + stdout +
                      '\n\n STDERR: ' + stderr)

        cleanup(tb_pid, tb_hdfs_path)

    return _wrapper_fun

def cleanup(tb_pid, tb_hdfs_path):
    hopshdfs.log('Performing cleanup')
    #if tb_pid != 0:
    #subprocess.Popen(["kill", str(tb_pid)])

    handle = hopshdfs.get()
    if not tb_hdfs_path == None and not tb_hdfs_path == '' and handle.exists(tb_hdfs_path):
        handle.delete(tb_hdfs_path)
    tensorboard.store()
    tensorboard.clean()
    hopshdfs.kill_logger()