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
import time

from hops import hdfs as hopshdfs
from hops import tensorboard
from hops import devices
from hops import util

run_id = 0

def launch(sc, notebook, local_logdir=False):
    """ Run notebook pointed to in HopsFS as a python file in mpirun
    Args:
      :spark_session: SparkSession object
      :notebook: The path in HopsFS to the notebook
    """
    global run_id
    print('\nStarting TensorFlow job, follow your progress on TensorBoard in Jupyter UI! \n')
    sys.stdout.flush()

    app_id = str(sc.applicationId)

    conf_num = int(sc._conf.get("spark.executor.instances"))

    #Each TF task should be run on 1 executor
    nodeRDD = sc.parallelize(range(1), 1)

    #Force execution on executor, since GPU is located on executor
    nodeRDD.foreachPartition(prepare_func(app_id, run_id, notebook, local_logdir))

    print('Finished Experiment \n')
    print('\nSee /Logs/TensorFlow/' + app_id + '/horovod/run.' + str(run_id) + ' for summary of results, logfiles and TensorBoard log directory')

def get_logdir(app_id):
    global run_id
    return hopshdfs.project_path() + 'Logs/TensorFlow/' + app_id + '/horovod/run.' + str(run_id)

def prepare_func(app_id, run_id, nb_path, local_logdir):

    def _wrapper_fun(iter):

        for i in iter:
            executor_num = i

        hdfs_exec_logdir, hdfs_appid_logdir = hopshdfs.create_directories(app_id, run_id, None, 'horovod')


        tb_pid = 0
        tb_hdfs_path = ''

        pydoop.hdfs.dump('', os.environ['EXEC_LOGFILE'], user=hopshdfs.project_user())
        hopshdfs.init_logger()
        hopshdfs.log('Starting Spark executor with arguments')
        if executor_num == 0:
            tb_hdfs_path, tb_pid = tensorboard.register(hdfs_exec_logdir, hdfs_appid_logdir, 0, local_logdir=local_logdir)


        gpu_str = '\n\nChecking for GPUs in the environment\n' + devices.get_gpu_info()
        hopshdfs.log(gpu_str)
        print(gpu_str)

        #1. Download notebook file
        fs_handle = hopshdfs.get_fs()

        try:
            fd = fs_handle.open_file(nb_path, flags='r')
        except:
            fd = fs_handle.open_file(nb_path, mode='r')

        notebook = ''
        for line in fd:
            if isinstance(line, bytes):
                notebook += line.decode("ascii")
            else:
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

        t_gpus = threading.Thread(target=devices.print_periodic_gpu_utilization)
        if devices.get_num_gpus() > 0:
            t_gpus.start()


        mpi_logfile_path = os.getcwd() + '/mpirun.log'
        if os.path.exists(mpi_logfile_path):
            os.remove(mpi_logfile_path)

        mpi_logfile = open(mpi_logfile_path, 'w')

        # 4. Run allreduce
        mpi_np = os.environ['MPI_NP']
        mpi_cmd = 'HOROVOD_TIMELINE=' + tensorboard.logdir() + '/timeline.json' + \
                  ' TENSORBOARD_LOGDIR=' + tensorboard.logdir() + \
                  ' mpirun -np ' + str(mpi_np) + \
                  ' -bind-to none -map-by slot ' + \
                  ' -x HOROVOD_TIMELINE ' + \
                  ' -x TENSORBOARD_LOGDIR ' + \
                  ' -x NCCL_DEBUG=INFO ' + \
                  os.environ['PYSPARK_PYTHON'] + ' ' + py_runnable
        mpi = subprocess.Popen(mpi_cmd,
                               shell=True,
                               stdout=mpi_logfile,
                               stderr=mpi_logfile,
                               preexec_fn=util.on_executor_exit('SIGTERM'))

        t_log = threading.Thread(target=print_log)
        t_log.start()

        mpi.wait()

        if devices.get_num_gpus() > 0:
            t_gpus.do_run = False
            t_gpus.join()

        return_code = mpi.returncode


        if local_logdir:
            local_tb = tensorboard.local_logdir_path
            pydoop.hdfs.put(local_tb, hdfs_exec_logdir)

        if return_code != 0:
            cleanup(tb_hdfs_path)
            t_log.do_run = False
            t_log.join()
            raise Exception('mpirun FAILED, look in the logs for the error')

        cleanup(tb_hdfs_path)
        t_log.do_run = False
        t_log.join()

        hopshdfs.kill_logger()

    return _wrapper_fun

def print_log():
    mpi_logfile_path = os.getcwd() + '/mpirun.log'
    mpi_logfile = open(mpi_logfile_path, 'r')
    t = threading.currentThread()
    while getattr(t, "do_run", True):
        where = mpi_logfile.tell()
        line = mpi_logfile.readline()
        if not line:
            time.sleep(1)
            mpi_logfile.seek(where)
        else:
            print(line)

    # Get the last outputs
    line = mpi_logfile.readline()
    while line:
        where = mpi_logfile.tell()
        print(line)
        line = mpi_logfile.readline()
        mpi_logfile.seek(where)



def cleanup(tb_hdfs_path):
    hopshdfs.log('Performing cleanup')
    handle = hopshdfs.get()
    if not tb_hdfs_path == None and not tb_hdfs_path == '' and handle.exists(tb_hdfs_path):
        handle.delete(tb_hdfs_path)

