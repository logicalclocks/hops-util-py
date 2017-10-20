import pydoop.hdfs
import subprocess
from ctypes import cdll
import os
import signal

from hops import hdfs as hopshdfs
from hops import tensorboard
from hops import devices

run_id = 0

def launch(spark_session):

    #Temporary crap fix
    os.environ['CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['CLASSPATH']
    os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['SPARK_DIST_CLASSPATH']
    os.environ['CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.jar:" + os.environ['CLASSPATH']
    os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.jar:" + os.environ['SPARK_DIST_CLASSPATH']
    os.environ['CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['CLASSPATH']
    os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['SPARK_DIST_CLASSPATH']
    #os.environ['HADOOP_CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.jar:" + os.environ['HADOOP_CLASSPATH']

    sc = spark_session.sparkContext
    app_id = str(sc.applicationId)

    conf_num = int(sc._conf.get("spark.executor.instances"))

    #Each TF task should be run on 1 executor
    nodeRDD = sc.parallelize(range(conf_num), conf_num)

    #Force execution on executor, since GPU is located on executor
    #function = getsource(map_fun)
    #function += '\n' + function.__name__ + '()'

    nodeRDD.foreachPartition(prepare_func(app_id, run_id))

    global run_id
    run_id += 1

def prepare_func(app_id, run_id):

    def _wrapper_fun(iter):

        for i in iter:
            executor_num = i

        #Temporary crap fix
        os.environ['CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['CLASSPATH']
        os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['SPARK_DIST_CLASSPATH']
        os.environ['CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.jar:" + os.environ['CLASSPATH']
        os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.jar:" + os.environ['SPARK_DIST_CLASSPATH']
        os.environ['CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['CLASSPATH']
        os.environ['SPARK_DIST_CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.1.jar:" + os.environ['SPARK_DIST_CLASSPATH']
        #os.environ['HADOOP_CLASSPATH'] = "/srv/hops-gpu/hadoop/share/hadoop/hdfs/lib/hops-leader-election-2.8.2.jar:" + os.environ['HADOOP_CLASSPATH']

        hdfs_exec_logdir, hdfs_appid_logdir = hopshdfs.create_directories(app_id, run_id, 0)

        tb_pid = 0
        tb_hdfs_path = ''

        pydoop.hdfs.dump('', os.environ['EXEC_LOGFILE'], user=hopshdfs.project_user())
        hopshdfs.init_logger()
        hopshdfs.log('Starting Spark executor with arguments')
        tb_hdfs_path, tb_pid = tensorboard.register(hdfs_exec_logdir, hdfs_appid_logdir, 0)
        gpu_str = '\nChecking for GPUs in the environment' + devices.get_gpu_info()
        hopshdfs.log(gpu_str)

        #1. Download notebook file
        proj_path = hopshdfs.project_path()
        proj_path += '/Jupyter'
        proj_path += '/all_reduce.ipynb'
        fs_handle = hopshdfs.get_fs()
        fd = fs_handle.open_file(proj_path, flags='r')
        notebook = ''
        for line in fd:
            notebook += line

        f_nb = open("all_reduce.ipynb","w+")
        f_nb.write(notebook)
        f_nb.flush()
        f_nb.close()

        os.listdir(os.getcwd())

        #2. Convert notebook to all_reduce.py file
        subprocess.check_call(['jupyter nbconvert --to python all_reduce.ipynb'])


        #3. Run allreduce
        subprocess.check_call(['mpirun -np ' + devices.get_num_gpus() + ' all_reduce.py'], preexec_fn=on_parent_exit('SIGTERM'),
                         stdout=open('out', 'w+'), stdstderr=subprocess.STDOUT, shell=True)

        cleanup(tb_pid, tb_hdfs_path)

    return _wrapper_fun

def on_parent_exit(signame):

    """
    Return a function to be run in a child process which will trigger
    SIGNAME to be sent when the parent process dies
    """
    signum = getattr(signal, signame)
    def set_parent_exit_signal():
        # http://linux.die.net/man/2/prctl
        result = cdll['libc.so.6'].prctl(PR_SET_PDEATHSIG, signum)
        if result != 0:
            raise Exception('prctl failed with error code %s' % result)
    return set_parent_exit_signal

def cleanup(tb_pid, tb_hdfs_path):
    hopshdfs.log('Performing cleanup')
    if tb_pid != 0:
        subprocess.Popen(["kill", str(tb_pid)])
        handle = hopshdfs.get()
        handle.delete(tb_hdfs_path)
        tensorboard.store()
        tensorboard.clean()
        hopshdfs.kill_logger()