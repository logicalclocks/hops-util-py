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
import socket


from hops import hdfs as hopshdfs
from hops import tensorboard
from hops import devices
from hops import util
import coordination_server

run_id = 0

def launch(spark_session, notebook, name="no-name"):
    """ Run notebook pointed to in HopsFS as a python file in mpirun
    Args:
      :spark_session: SparkSession object
      :notebook: The path in HopsFS to the notebook
    """
    global run_id

    print('\nStarting TensorFlow job, follow your progress on TensorBoard in Jupyter UI! \n')
    sys.stdout.flush()

    sc = spark_session.sparkContext
    app_id = str(sc.applicationId)

    conf_num = int(sc._conf.get("spark.executor.instances"))

    #Each TF task should be run on 1 executor
    nodeRDD = sc.parallelize(range(conf_num), conf_num)

    server = coordination_server.Server(conf_num)
    server_addr = server.start()

    #Make SparkUI intuitive by grouping jobs
    sc.setJobGroup("Horovod training", "{} | Horovod training".format(name))

    #Force execution on executor, since GPU is located on executor
    nodeRDD.foreachPartition(prepare_func(app_id, run_id, notebook, server_addr))

    print('Finished TensorFlow job \n')
    print('Make sure to check /Logs/TensorFlow/' + app_id + '/runId.' + str(run_id) + ' for logfile and TensorBoard logdir')

def get_logdir(app_id):
    global run_id
    return hopshdfs.project_path() + '/Logs/TensorFlow/' + app_id + '/horovod/run.' + str(run_id)

def prepare_func(app_id, run_id, nb_path, server_addr):

    def _wrapper_fun(iter):

        for i in iter:
            executor_num = i

        client = coordination_server.Client(server_addr)

        node_meta = {'host': get_ip_address(),
                     'executor_cwd': os.getcwd(),
                     'cuda_visible_devices_ordinals': devices.get_minor_gpu_device_numbers()}

        client.register(node_meta)

        t_gpus = threading.Thread(target=devices.print_periodic_gpu_utilization)
        if devices.get_num_gpus() > 0:
            t_gpus.start()

        # Only spark executor with index 0 should create necessary HDFS directories and start mpirun
        # Other executors simply block until index 0 reports mpirun is finished

        clusterspec = client.await_reservations()

        #pydoop.hdfs.dump('', os.environ['EXEC_LOGFILE'], user=hopshdfs.project_user())
        #hopshdfs.init_logger()
        #hopshdfs.log('Starting Spark executor with arguments')

        gpu_str = '\n\nChecking for GPUs in the environment\n' + devices.get_gpu_info()
        #hopshdfs.log(gpu_str)
        print(gpu_str)

        mpi_logfile_path = os.getcwd() + '/mpirun.log'
        if os.path.exists(mpi_logfile_path):
            os.remove(mpi_logfile_path)

        mpi_logfile = open(mpi_logfile_path, 'w')

        py_runnable = localize_scripts(nb_path, clusterspec)

        # non-chief executor should not do mpirun
        if not executor_num == 0:
            client.await_mpirun_finished()
        else:
            hdfs_exec_logdir, hdfs_appid_logdir = hopshdfs.create_directories(app_id, run_id, param_string='Horovod')
            tb_hdfs_path, tb_pid = tensorboard.register(hdfs_exec_logdir, hdfs_appid_logdir, 0)

            mpi_cmd = 'HOROVOD_TIMELINE=' + tensorboard.logdir() + '/timeline.json' + \
                      ' TENSORBOARD_LOGDIR=' + tensorboard.logdir() + \
                      ' mpirun -np ' + str(get_num_ps(clusterspec)) + ' --hostfile ' + get_hosts_file(clusterspec) + \
                      ' -bind-to none -map-by slot ' + \
                      ' -x LD_LIBRARY_PATH ' + \
                      ' -x HOROVOD_TIMELINE ' + \
                      ' -x TENSORBOARD_LOGDIR ' + \
                      ' -x NCCL_DEBUG=INFO ' + \
                      ' -mca pml ob1 -mca btl ^openib ' + \
                      os.environ['PYSPARK_PYTHON'] + ' ' + py_runnable

            mpi = subprocess.Popen(mpi_cmd,
                                   shell=True,
                                   stdout=mpi_logfile,
                                   stderr=mpi_logfile,
                                   preexec_fn=util.on_executor_exit('SIGTERM'))

            t_log = threading.Thread(target=print_log)
            t_log.start()

            mpi.wait()

            client.register_mpirun_finished()

            if devices.get_num_gpus() > 0:
                t_gpus.do_run = False
                t_gpus.join()

            return_code = mpi.returncode

            if return_code != 0:
                cleanup(tb_hdfs_path)
                t_log.do_run = False
                t_log.join()
                raise Exception('mpirun FAILED, look in the logs for the error')

            cleanup(tb_hdfs_path)
            t_log.do_run = False
            t_log.join()

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
            print line

    # Get the last outputs
    line = mpi_logfile.readline()
    while line:
        where = mpi_logfile.tell()
        print line
        line = mpi_logfile.readline()
        mpi_logfile.seek(where)


def cleanup(tb_hdfs_path):
    hopshdfs.log('Performing cleanup')
    handle = hopshdfs.get()
    if not tb_hdfs_path == None and not tb_hdfs_path == '' and handle.exists(tb_hdfs_path):
        handle.delete(tb_hdfs_path)
    hopshdfs.kill_logger()

def get_ip_address():
    """Simple utility to get host IP address"""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    return s.getsockname()[0]

def get_hosts_string(clusterspec):
    hosts_string = ''
    for host in clusterspec:
        hosts_string = hosts_string + ' ' + host['host'] + ':' + str(len(host['cuda_visible_devices_ordinals']))

def get_num_ps(clusterspec):
    num = 0
    for host in clusterspec:
        num += len(host['cuda_visible_devices_ordinals'])
    return num

def get_hosts_file(clusterspec):
    hf = ''
    host_file = os.getcwd() + '/host_file'
    for host in clusterspec:
        hf = hf + '\n' + host['host'] + ' ' + 'slots=' + str(len(host['cuda_visible_devices_ordinals']))
    with open(host_file, 'w') as hostfile: hostfile.write(hf)
    return host_file

def find_host_in_clusterspec(clusterspec, host):
    for h in clusterspec:
        if h['name'] == host:
            return h

# The code generated by this function will be called in an eval, which changes the working_dir and cuda_visible_devices for process running mpirun
def generate_environment_script(clusterspec):

    import_script = 'import os \n' \
                    'from hops import util'

    export_script = ''

    for host in clusterspec:
        export_script += 'def export_workdir():\n' \
                         '    if util.get_ip_address() == \"' + find_host_in_clusterspec(clusterspec, host['host'])['host'] + '\":\n' \
                                                                                                                              '        os.chdir=\"' + host['executor_cwd'] + '\"\n' \
                                                                                                                                                                             '        os.environ["CUDA_DEVICE_ORDER"]=\"PCI_BUS_ID\" \n' \
                                                                                                                                                                             '        os.environ["CUDA_VISIBLE_DEVICES"]=\"' + ",".join(str(x) for x in host['cuda_visible_devices_ordinals']) + '\"\n'

    return import_script + '\n' + export_script

def localize_scripts(nb_path, clusterspec):

    # 1. Download the notebook as a string
    fs_handle = hopshdfs.get_fs()
    fd = fs_handle.open_file(nb_path, flags='r')
    note = fd.read()
    fd.close()

    path, filename = os.path.split(nb_path)
    f_nb = open(filename,"w+")
    f_nb.write(note)
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

    # 3. Prepend script to export environment variables and Make py file runnable
    py_runnable = os.getcwd() + '/' + filename.split('.')[0] + '.py'

    notebook = 'with open("generate_env.py", "r") as myfile:\n' \
               '    data=myfile.read()\n' \
               '    exec(data)\n'
    with open(py_runnable, 'r') as original: data = original.read()
    with open(py_runnable, 'w') as modified: modified.write(notebook + data)

    st = os.stat(py_runnable)
    os.chmod(py_runnable, st.st_mode | stat.S_IEXEC)

    # 4. Localize generate_env.py script
    environment_script = generate_environment_script(clusterspec)
    generate_env_path = os.getcwd() + '/generate_env.py'
    f_env = open(generate_env_path, "w+")
    f_env.write(environment_script)
    f_env.flush()
    f_env.close()

    # 5. Make generate_env.py runnable
    st = os.stat(generate_env_path)
    os.chmod(py_runnable, st.st_mode | stat.S_IEXEC)

    return py_runnable