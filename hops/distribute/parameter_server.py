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
    """

    Args:
        sc:
        map_fun:
        local_logdir:
        name:

    Returns:

    """
    global run_id
    app_id = str(sc.applicationId)

    num_executions = int(sc._conf.get("spark.executor.instances"))

    #Each TF task should be run on 1 executor
    nodeRDD = sc.parallelize(range(num_executions), num_executions)

    #Make SparkUI intuitive by grouping jobs
    sc.setJobGroup("ParameterServerStrategy", "{} | Distributed Training".format(name))

    server = parameter_server_reservation.Server(num_executions)
    server_addr = server.start()

    num_ps = util.num_param_servers()

    #Force execution on executor, since GPU is located on executor
    nodeRDD.foreachPartition(_prepare_func(app_id, run_id, map_fun, local_logdir, server_addr, num_ps))

    logdir = _get_logdir(app_id)

    path_to_metric = logdir + '/metric'
    if pydoop.hdfs.path.exists(path_to_metric):
        with pydoop.hdfs.open(path_to_metric, "r") as fi:
            metric = float(fi.read())
            fi.close()
            return metric, logdir

    print('Finished Experiment \n')

    return None, logdir

def _get_logdir(app_id):
    """

    Args:
        app_id:

    Returns:

    """
    global run_id
    return hopshdfs._get_experiments_dir() + '/' + app_id + '/parameter_server/run.' + str(run_id)

def _prepare_func(app_id, run_id, map_fun, local_logdir, server_addr, num_ps):
    """

    Args:
        app_id:
        run_id:
        map_fun:
        local_logdir:
        server_addr:
        num_ps:

    Returns:

    """

    def _wrapper_fun(iter):
        """

        Args:
            iter:

        Returns:

        """

        for i in iter:
            executor_num = i

        tb_hdfs_path = ''
        hdfs_exec_logdir = ''

        t = threading.Thread(target=devices._print_periodic_gpu_utilization)
        if devices.get_num_gpus() > 0:
            t.start()

        role = None

        client = parameter_server_reservation.Client(server_addr)

        try:
            host = util._get_ip_address()

            tmp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tmp_socket.bind(('', 0))
            port = tmp_socket.getsockname()[1]
            host_port = host + ":" + str(port)

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

            role, index = _find_task_and_index(host_port, cluster)

            cluster_spec = {}
            cluster_spec["cluster"] = cluster
            cluster_spec["task"] = {"type": role, "index": index}

            print(cluster_spec)

            os.environ["TF_CONFIG"] = json.dumps(cluster_spec)

            if role == "chief":
                hdfs_exec_logdir, hdfs_appid_logdir = hopshdfs._create_directories(app_id, run_id, None, 'parameter_server')
                pydoop.hdfs.dump('', os.environ['EXEC_LOGFILE'], user=hopshdfs.project_user())
                hopshdfs._init_logger()
                tb_hdfs_path, tb_pid = tensorboard._register(hdfs_exec_logdir, hdfs_appid_logdir, executor_num, local_logdir=local_logdir)
            gpu_str = '\nChecking for GPUs in the environment' + devices._get_gpu_info()
            if role == "chief":
                hopshdfs.log(gpu_str)
            print(gpu_str)
            print('-------------------------------------------------------')
            print('Started running task \n')
            if role == "chief":
                hopshdfs.log('Started running task')
            task_start = datetime.datetime.now()

            retval=None
            if role == "ps":
                ps_thread = threading.Thread(target=lambda: map_fun())
                ps_thread.start()
                print("waiting for workers")
                client.await_all_workers_finished()
                print("waiting finished")
            else:
                retval = map_fun()

            if role == "chief":
                if retval:
                    _handle_return(retval, hdfs_exec_logdir)

            task_end = datetime.datetime.now()
            time_str = 'Finished task - took ' + util._time_diff(task_start, task_end)
            print('\n' + time_str)
            print('-------------------------------------------------------')
            if role == "chief":
                hopshdfs.log(time_str)
        except:
            _cleanup(tb_hdfs_path)
            if devices.get_num_gpus() > 0:
                t.do_run = False
                t.join()
            raise
        finally:
            if role == "chief":
                if local_logdir:
                    local_tb = tensorboard.local_logdir_path
                    util._store_local_tensorboard(local_tb, hdfs_exec_logdir)
            try:
                if role == "worker" or role == "chief":
                    client.register_worker_finished()
                client.close()
            except:
                pass

        _cleanup(tb_hdfs_path)
        if devices.get_num_gpus() > 0:
            t.do_run = False
            t.join()

    return _wrapper_fun

def _cleanup(tb_hdfs_path):
    """

    Args:
        tb_hdfs_path:

    Returns:

    """
    handle = hopshdfs.get()
    if not tb_hdfs_path == None and not tb_hdfs_path == '' and handle.exists(tb_hdfs_path):
        handle.delete(tb_hdfs_path)
    hopshdfs._kill_logger()

def _find_task_and_index(host_port, cluster_spec):
    """

    Args:
        host_port:
        cluster_spec:

    Returns:

    """
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

def _handle_return(val, hdfs_exec_logdir):
    """

    Args:
        val:
        hdfs_exec_logdir:

    Returns:

    """
    try:
        test = int(val)
    except:
        raise ValueError('Your function needs to return a metric (number) which should be maximized or minimized')

    metric_file = hdfs_exec_logdir + '/metric'
    fs_handle = hopshdfs.get_fs()
    try:
        fd = fs_handle.open_file(metric_file, mode='w')
    except:
        fd = fs_handle.open_file(metric_file, flags='w')
    fd.write(str(float(val)).encode())
    fd.flush()
    fd.close()

