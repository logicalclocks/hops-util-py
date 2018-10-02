"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import os
from hops import hdfs as hopshdfs
from hops import tensorboard
from hops import devices
from hops import util

import pydoop.hdfs
import threading
import datetime
import socket
import json

from . import parameter_server_reservation

run_id = 0

def _launch(sc, map_fun, local_logdir=False, name="no-name"):
    global run_id
    app_id = str(sc.applicationId)

    num_executions = int(sc._conf.get("spark.executor.instances"))

    #Each TF task should be run on 1 executor
    nodeRDD = sc.parallelize(range(num_executions), num_executions)

    #Make SparkUI intuitive by grouping jobs
    sc.setJobGroup("TensorFlow ParameterServerStrategy", "{} | Distributed Training".format(name))

    server = parameter_server_reservation.Server(num_executions)
    server_addr = server.start()

    num_ps = util.num_param_servers()

    #Force execution on executor, since GPU is located on executor
    nodeRDD.foreachPartition(_prepare_func(app_id, run_id, map_fun, local_logdir, server_addr, num_ps))

    print('Finished Experiment \n')

    return None

def get_logdir(app_id):
    global run_id
    return hopshdfs.get_experiments_dir() + '/' + app_id + '/parameter_server/run.' + str(run_id)

def _prepare_func(app_id, run_id, map_fun, local_logdir, server_addr, num_ps):

    def _wrapper_fun(iter):

        for i in iter:
            executor_num = i

        tb_hdfs_path = ''
        hdfs_exec_logdir = ''

        t = threading.Thread(target=devices.print_periodic_gpu_utilization)
        if devices.get_num_gpus() > 0:
            t.start()

        role = None

        try:
            host = util.get_ip_address()

            tmp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tmp_socket.bind(('', 0))
            port = tmp_socket.getsockname()[1]
            host_port = host + ":" + str(port)

            client = parameter_server_reservation.Client(server_addr)

            exec_spec = {}
            if executor_num < num_ps:
                exec_spec["task_type"] = "ps"
            else:
                exec_spec["task_type"] = "worker"
            exec_spec["host_port"] = host_port
            exec_spec["gpus_present"] = devices.get_num_gpus() > 0

            client.register(exec_spec)

            cluster = client.await_reservations()

            tmp_socket.close()
            client.close()

            role, index = _find_task_and_index(host_port, cluster)

            cluster_spec = {}
            cluster_spec["cluster"] = cluster
            cluster_spec["task"] = {"type": role, "index": index}

            print(cluster_spec)

            os.environ["TF_CONFIG"] = json.dumps(cluster_spec)

            if role == "chief":
                hdfs_exec_logdir, hdfs_appid_logdir = hopshdfs.create_directories(app_id, run_id, None, 'parameter_server')
                pydoop.hdfs.dump('', os.environ['EXEC_LOGFILE'], user=hopshdfs.project_user())
                hopshdfs.init_logger()
                tb_hdfs_path, tb_pid = tensorboard.register(hdfs_exec_logdir, hdfs_appid_logdir, executor_num, local_logdir=local_logdir)
            gpu_str = '\nChecking for GPUs in the environment' + devices.get_gpu_info()
            if role == "chief":
                hopshdfs.log(gpu_str)
            print(gpu_str)
            print('-------------------------------------------------------')
            print('Started running task \n')
            if role == "chief":
                hopshdfs.log('Started running task')
            task_start = datetime.datetime.now()

            retval = map_fun()
            task_end = datetime.datetime.now()
            time_str = 'Finished task - took ' + util.time_diff(task_start, task_end)
            print('\n' + time_str)
            print('-------------------------------------------------------')
            if role == "chief":
                hopshdfs.log(time_str)
        except:
            #Always do cleanup
            _cleanup(tb_hdfs_path)
            if devices.get_num_gpus() > 0:
                t.do_run = False
                t.join()
            raise
        finally:
            if role == "chief":
                if local_logdir:
                    local_tb = tensorboard.local_logdir_path
                    util.store_local_tensorboard(local_tb, hdfs_exec_logdir)


        _cleanup(tb_hdfs_path)
        if devices.get_num_gpus() > 0:
            t.do_run = False
            t.join()

    return _wrapper_fun

def _cleanup(tb_hdfs_path):
    handle = hopshdfs.get()
    if not tb_hdfs_path == None and not tb_hdfs_path == '' and handle.exists(tb_hdfs_path):
        handle.delete(tb_hdfs_path)
    hopshdfs.kill_logger()

def _find_task_and_index(host_port, cluster_spec):

    index = 0
    for entry in cluster_spec["worker"]:
        if entry == host_port:
            return "worker", index
        index = index + 1

    index = 0
    for entry in cluster_spec["ps"]:
        if entry == host_port:
            return "ps", index
        index = index + 1


    if cluster_spec["chief"][0] == host_port:
       return "chief", 0

