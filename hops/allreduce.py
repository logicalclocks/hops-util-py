import pydoop.hdfs
import subprocess
from ctypes import cdll
import os
import signal

from hops import hdfs as hopshdfs
from hops import tensorboard
from hops import devices

from dill.source import getsource

def launch(spark_session, map_fun):

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


    arg_lists = args_dict.values()
    currentLen = len(arg_lists[0])
    for i in range(len(arg_lists)):
        if currentLen != len(arg_lists[i]):
            raise ValueError('Length of each function argument list must be equal')
        currentLen = len(arg_lists[i])

    num_executors = currentLen
    conf_num = int(sc._conf.get("spark.executor.instances"))
    if conf_num < num_executors:
        raise ValueError(('You configured {0} executors, but trying to run {1} TensorFlow jobs'
                         + ' increase number of executors or reduce number of TensorFlow jobs.')
                         .format(conf_num, num_executors))

    #Each TF task should be run on 1 executor
    nodeRDD = sc.parallelize(range(num_executors), num_executors)

    #Force execution on executor, since GPU is located on executor
    nodeRDD.foreachPartition(prepare_func(app_id, run_id, map_fun))

    global run_id
    run_id += 1

def prepare_func(app_id, run_id, map_fun):

    def _wrapper_fun(iter):

        function = getsource(map_fun)
        function += '\n' + function.__name__ + '()'

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

        f = open("python.py","w+")
        f.write(function)
        f.close()

        subprocess.check_call(['mpirun -np ' + devices.get_num_gpus() + ' python.py'], preexec_fn=on_parent_exit('SIGTERM'),
                         stdout=open('out', 'w+'), stdstderr=subprocess.STDOUT, shell=True)

        cleanup(tb_pid, tb)

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