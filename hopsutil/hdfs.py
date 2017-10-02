"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import pydoop.hdfs as hdfs
import os
import datetime

def get():
    return hdfs.hdfs('default', 0, user=project_user())

def get_fs():
    return hdfs.fs.hdfs('default', 0, user=project_user())

def project_path():
    hops_user = os.environ["SPARK_USER"]
    hops_user_split = hops_user.split("__")
    project = hops_user_split[0]
    return hdfs.path.abspath("/Projects/" + project + "/")

def project_user():
    hops_user = os.environ["SPARK_USER"]
    return hops_user

def log(string):
    fs_handle = get_fs()
    if isinstance(string, basestring):
        logfile = os.environ['EXEC_LOGFILE']
        with fs_handle.open_file(logfile, flags='w') as f:
            f.seek(f.size)
            f.write('{0}: {1}'.format(datetime.datetime.now().isoformat(), string) + '\n')
    else:
        logfile = os.environ['EXEC_LOGFILE']
        with fs_handle.open_file(logfile, flags='w') as f:
            f.seek(f.size)
            f.write('{0}: {1}'.format(datetime.datetime.now().isoformat(),
            'ERROR! Attempting to write a non-basestring object to logfile') + '\n')

def create_directories(app_id, run_id, executor_num):
    pyhdfs_handle = get()
    #Create output directory for TensorBoard events for this executor
    #REMOVE THIS LATER!!!!!!!!!! Folder should be created automatically
    hdfs_events_parent_dir = project_path() + "/Logs/Tensorboard"
    if not pyhdfs_handle.exists(hdfs_events_parent_dir):
        pyhdfs_handle.create_directory(hdfs_events_parent_dir)

    hdfs_appid_logdir = hdfs_events_parent_dir + "/" + app_id
    if not pyhdfs_handle.exists(hdfs_appid_logdir):
        pyhdfs_handle.create_directory(hdfs_appid_logdir)

    hdfs_run_id_logdir = hdfs_appid_logdir + "/" + "runId." + str(run_id)
    if not pyhdfs_handle.exists(hdfs_run_id_logdir):
        pyhdfs_handle.create_directory(hdfs_run_id_logdir)

    logfile = hdfs_run_id_logdir + '/execuctor.' + str(executor_num) + '.log'
    os.environ['EXEC_LOGFILE'] = logfile

    hdfs_exec_logdir = hdfs_run_id_logdir + "/executor." + str(executor_num)
    if not pyhdfs_handle.exists(hdfs_exec_logdir):
        pyhdfs_handle.create_directory(hdfs_exec_logdir)

    return hdfs_exec_logdir, hdfs_appid_logdir