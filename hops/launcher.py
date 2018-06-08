from hops import util
from hops import hdfs as hopshdfs
import pydoop.hdfs as hdfs
from hops import tensorboard
from hops import devices

import pydoop.hdfs
import threading
import six
import datetime
import os

run_id = 0


def launch(sc, map_fun, args_dict=None, local_logdir=False):

    global run_id

    app_id = str(sc.applicationId)


    if args_dict == None:
        num_executions = 1
    else:
        arg_lists = list(args_dict.values())
        currentLen = len(arg_lists[0])
        for i in range(len(arg_lists)):
            if currentLen != len(arg_lists[i]):
                raise ValueError('Length of each function argument list must be equal')
            num_executions = len(arg_lists[i])

    #Each TF task should be run on 1 executor
    nodeRDD = sc.parallelize(range(num_executions), num_executions)

    #Force execution on executor, since GPU is located on executor    global run_id
    nodeRDD.foreachPartition(_prepare_func(app_id, run_id, map_fun, args_dict, local_logdir))

    print('Finished Experiment \n')
    print('See /Logs/TensorFlow/' + app_id + '/launcher/run.' + str(run_id) + ' for logfile and contents of TensorBoard logdir')

    if args_dict == None:
        path_to_metric = get_logdir(app_id) + '/no_args/metric'
        if pydoop.hdfs.path.exists(path_to_metric):
            with pydoop.hdfs.open(path_to_metric, "r") as fi:
                metric = float(fi.read())
                fi.close()
                return metric, 'hdfs:///Projects/' + hopshdfs.project_name() + '/Logs/TensorFlow/' + app_id + '/launcher/run.' +  str(run_id)

    return None, 'hdfs:///Projects/' + hopshdfs.project_name() + '/Logs/TensorFlow/' + app_id + '/launcher/run.' +  str(run_id)

def get_logdir(app_id):
    global run_id
    return hopshdfs.project_path() + '/Logs/TensorFlow/' + app_id + '/launcher/run.' +  str(run_id)


#Helper to put Spark required parameter iter in function signature
def _prepare_func(app_id, run_id, map_fun, args_dict, local_logdir):

    def _wrapper_fun(iter):

        for i in iter:
            executor_num = i

        tb_pid = 0
        tb_hdfs_path = ''
        hdfs_exec_logdir = ''

        t = threading.Thread(target=devices.print_periodic_gpu_utilization)
        if devices.get_num_gpus() > 0:
            t.start()

        try:
            #Arguments
            if args_dict:
                argcount = six.get_function_code(map_fun).co_argcount
                names = six.get_function_code(map_fun).co_varnames

                args = []
                argIndex = 0
                param_string = ''
                while argcount > 0:
                    #Get args for executor and run function
                    param_name = names[argIndex]
                    param_val = args_dict[param_name][executor_num]
                    param_string += str(param_name) + '=' + str(param_val) + '.'
                    args.append(param_val)
                    argcount -= 1
                    argIndex += 1
                param_string = param_string[:-1]
                hdfs_exec_logdir, hdfs_appid_logdir = hopshdfs.create_directories(app_id, run_id, param_string, 'launcher')
                pydoop.hdfs.dump('', os.environ['EXEC_LOGFILE'], user=hopshdfs.project_user())
                hopshdfs.init_logger()
                tb_hdfs_path, tb_pid = tensorboard.register(hdfs_exec_logdir, hdfs_appid_logdir, executor_num, local_logdir=local_logdir)

                gpu_str = '\nChecking for GPUs in the environment' + devices.get_gpu_info()
                hopshdfs.log(gpu_str)
                print(gpu_str)
                print('-------------------------------------------------------')
                print('Started running task ' + param_string + '\n')
                hopshdfs.log('Started running task ' + param_string)
                task_start = datetime.datetime.now()
                map_fun(*args)
                task_end = datetime.datetime.now()
                time_str = 'Finished task ' + param_string + ' - took ' + util.time_diff(task_start, task_end)
                print('\n' + time_str)
                print('-------------------------------------------------------')
                hopshdfs.log(time_str)
            else:
                hdfs_exec_logdir, hdfs_appid_logdir = hopshdfs.create_directories(app_id, run_id, 'no_args', 'launcher')
                pydoop.hdfs.dump('', os.environ['EXEC_LOGFILE'], user=hopshdfs.project_user())
                hopshdfs.init_logger()
                tb_hdfs_path, tb_pid = tensorboard.register(hdfs_exec_logdir, hdfs_appid_logdir, executor_num, local_logdir=local_logdir)
                gpu_str = '\nChecking for GPUs in the environment' + devices.get_gpu_info()
                hopshdfs.log(gpu_str)
                print(gpu_str)
                print('-------------------------------------------------------')
                print('Started running task\n')
                hopshdfs.log('Started running task')
                task_start = datetime.datetime.now()
                retval = map_fun()
                task_end = datetime.datetime.now()
                if retval:
                    _handle_return(retval, hdfs_exec_logdir)
                time_str = 'Finished task - took ' + util.time_diff(task_start, task_end)
                print('\n' + time_str)
                print('-------------------------------------------------------')
                hopshdfs.log(time_str)
        except:
            #Always do cleanup
            _cleanup(tb_hdfs_path)
            if devices.get_num_gpus() > 0:
                t.do_run = False
                t.join()
            raise
        finally:
            try:
                if local_logdir:
                    local_tb = tensorboard.local_logdir_path
                    util.store_local_tensorboard(local_tb, hdfs_exec_logdir)
            except:
                pass

        _cleanup(tb_hdfs_path)
        if devices.get_num_gpus() > 0:
            t.do_run = False
            t.join()

    return _wrapper_fun

def _cleanup(tb_hdfs_path):
    global experiment_json
    handle = hopshdfs.get()
    if not tb_hdfs_path == None and not tb_hdfs_path == '' and handle.exists(tb_hdfs_path):
        handle.delete(tb_hdfs_path)
    hopshdfs.kill_logger()

def _handle_return(val, hdfs_exec_logdir):
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
