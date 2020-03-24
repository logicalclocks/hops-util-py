"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import os
from hops import devices, tensorboard, hdfs
from hops.experiment_impl.util import experiment_utils
from hops import util

import pydoop.hdfs
import threading
import time
import socket
import json

from . import allreduce_reservation

def _run(sc, map_fun, run_id, local_logdir=False, name="no-name", evaluator=False):
    """

    Args:
        sc:
        map_fun:
        local_logdir:
        name:

    Returns:

    """
    app_id = str(sc.applicationId)

    num_executions = util.num_executors()

    #Each TF task should be run on 1 executor
    nodeRDD = sc.parallelize(range(num_executions), num_executions)

    #Make SparkUI intuitive by grouping jobs
    sc.setJobGroup(os.environ['ML_ID'], "{} | CollectiveAllReduceStrategy - Distributed Training".format(name))

    server = allreduce_reservation.Server(num_executions)
    server_addr = server.start()

    #Force execution on executor, since GPU is located on executor
    nodeRDD.foreachPartition(_prepare_func(app_id, run_id, map_fun, local_logdir, server_addr, evaluator, util.num_executors()))

    logdir = experiment_utils._get_logdir(app_id, run_id)

    print('Finished Experiment \n')

    path_to_return = logdir + '/.outputs.json'
    if pydoop.hdfs.path.exists(path_to_return):
        with pydoop.hdfs.open(path_to_return, "r") as fi:
            contents = fi.read()
            fi.close()
            return logdir, json.loads(contents)

    return logdir, None

def _prepare_func(app_id, run_id, map_fun, local_logdir, server_addr, evaluator, num_executors):
    """

    Args:
        app_id:
        run_id:
        map_fun:
        local_logdir:
        server_addr:

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

        experiment_utils._set_ml_id(app_id, run_id)

        t = threading.Thread(target=devices._print_periodic_gpu_utilization)
        if devices.get_num_gpus() > 0:
            t.start()

        is_chief = False
        logdir = None
        tb_hdfs_path = None
        try:
            host = experiment_utils._get_ip_address()

            tmp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tmp_socket.bind(('', 0))
            port = tmp_socket.getsockname()[1]

            client = allreduce_reservation.Client(server_addr)
            host_port = host + ":" + str(port)

            client.register({"worker": host_port, "index": executor_num})
            cluster = client.await_reservations()
            tmp_socket.close()
            client.close()

            task_index = experiment_utils._find_index(host_port, cluster)

            if task_index == -1:
                cluster["task"] = {"type": "chief", "index": 0}
            else:
                cluster["task"] = {"type": "worker", "index": task_index}

            evaluator_node = None
            if evaluator:
                last_worker_index = len(cluster["cluster"]["worker"])-1
                evaluator_node = cluster["cluster"]["worker"][last_worker_index]
                cluster["cluster"]["evaluator"] = [evaluator_node]
                del cluster["cluster"]["worker"][last_worker_index]
                if evaluator_node == host_port:
                    cluster["task"] = {"type": "evaluator", "index": 0}

            print('TF_CONFIG: {} '.format(cluster))

            if num_executors > 1:
                os.environ["TF_CONFIG"] = json.dumps(cluster)

            is_chief = (cluster["task"]["type"] == "chief")

            is_evaluator = (cluster["task"]["type"] == "evaluator")

            if is_chief:
                logdir = experiment_utils._get_logdir(app_id, run_id)
                tb_hdfs_path, tb_pid = tensorboard._register(logdir, logdir, executor_num, local_logdir=local_logdir)
            elif is_evaluator:
                logdir = experiment_utils._get_logdir(app_id, run_id)
                tensorboard.events_logdir = logdir

            logfile = experiment_utils._init_logger(experiment_utils._get_logdir(app_id, run_id), role=cluster["task"]["type"], index=cluster["task"]["index"])

            print(devices._get_gpu_info())
            print('-------------------------------------------------------')
            print('Started running task')
            task_start = time.time()
            retval = map_fun()

            if is_chief:
                experiment_utils._handle_return_simple(retval, experiment_utils._get_logdir(app_id, run_id), logfile)

            task_end = time.time()
            time_str = 'Finished task - took ' + experiment_utils._time_diff(task_start, task_end)
            print(time_str)
            print('-------------------------------------------------------')
        except:
            raise
        finally:
            experiment_utils._cleanup(tensorboard, t)

    return _wrapper_fun