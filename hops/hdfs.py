"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import pydoop.hdfs as hdfs
import os
import datetime
from six import string_types

def get():
    """ Get a handle to pydoop hdfs

    Returns:
      Pydoop hdfs handle
    """
    return hdfs.hdfs('default', 0, user=project_user())

def get_fs():
    """ Get a handle to pydoop fs

    Returns:
      Pydoop fs handle
    """
    return hdfs.fs.hdfs('default', 0, user=project_user())

def project_path(project_name=None):
    """ Get the path in HopsFS where the HopsWorks project is located. To point to a particular dataset, this path should be
    appended with the name of your dataset.

    Args:
     :project_name: If this value is not specified, it will get the path to your project. If you need to path to another project,
     you can specify the name of the project as a string.
    """

    if project_name:
        return hdfs.path.abspath("/Projects/" + project_name + "/")
    hops_user = project_user()
    hops_user_split = hops_user.split("__")
    project = hops_user_split[0]
    return hdfs.path.abspath("/Projects/" + project + "/")

def project_user():
    hops_user = None
    try:
        hops_user = os.environ["HADOOP_USER_NAME"]
    except:
        hops_user = os.environ["HDFS_USER"]
    return hops_user

def project_name():
    hops_user = project_user()
    hops_user_split = hops_user.split("__")
    project = hops_user_split[0]
    return project

fd = None

def init_logger():
    logfile = os.environ['EXEC_LOGFILE']
    fs_handle = get_fs()
    global fd
    fd = fs_handle.open_file(logfile, mode='w')

def log(string):
    if fd:
        if isinstance(string, string_types):
            fd.write(('{0}: {1}'.format(datetime.datetime.now().isoformat(), string) + '\n').encode())
        else:
            fd.write(('{0}: {1}'.format(datetime.datetime.now().isoformat(),
            'ERROR! Attempting to write a non-string object to logfile') + '\n').encode())

def kill_logger():
    if fd:
        try:
            fd.flush()
            fd.close()
        except:
            pass

def create_directories(app_id, run_id, param_string, parent):

    pyhdfs_handle = get()
    #Create output directory for TensorBoard events for this executor
    #REMOVE THIS LATER!!!!!!!!!! Folder should be created automatically
    hdfs_events_parent_dir = project_path() + "/Logs/TensorFlow"
    #if not pyhdfs_handle.exists(hdfs_events_parent_dir):
    #pyhdfs_handle.create_directory(hdfs_events_parent_dir)

    hdfs_appid_logdir = hdfs_events_parent_dir + "/" + app_id
    #if not pyhdfs_handle.exists(hdfs_appid_logdir):
    #pyhdfs_handle.create_directory(hdfs_appid_logdir)

    hdfs_run_id_logdir = hdfs_appid_logdir + "/" + parent + "." + str(run_id)
    #if not pyhdfs_handle.exists(hdfs_run_id_logdir):
    #pyhdfs_handle.create_directory(hdfs_run_id_logdir)

    hdfs_exec_logdir = hdfs_run_id_logdir + "/" + str(param_string)
    #if not pyhdfs_handle.exists(hdfs_exec_logdir):
    pyhdfs_handle.create_directory(hdfs_exec_logdir)

    logfile = hdfs_exec_logdir + '/' + 'logfile'
    os.environ['EXEC_LOGFILE'] = logfile

    try:
        hdfs.chmod(hdfs_events_parent_dir, "g+w")
    except IOError:
        # If this happens then the permission is set correct already since the creator of the /Logs/TensorFlow already set group writable
        pass

    return hdfs_exec_logdir, hdfs_appid_logdir

def copy_to_project(local_path, relative_hdfs_path):

    if "PDIR" in os.environ:
        full_local = os.environ['PDIR'] + '/' + local_path
    else:
        full_local = os.getcwd() + '/' + local_path

    proj_path = project_path()
    project_hdfs_path = proj_path + '/' + relative_hdfs_path

    hdfs.put(full_local, project_hdfs_path)

def copy_from_project(relative_hdfs_path, local_path):

    if "PDIR" in os.environ:
        full_local = os.environ['PDIR'] + '/' + local_path
    else:
        full_local = os.getcwd() + '/' + local_path

    proj_path = project_path()
    project_hdfs_path = proj_path + '/' + relative_hdfs_path

    hdfs.get(project_hdfs_path, full_local)