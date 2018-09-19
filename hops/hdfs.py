"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import pydoop.hdfs as hdfs
import os
import datetime
from six import string_types
import shutil
import stat
import fnmatch
import os
import errno

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
    try:
        fd = fs_handle.open_file(logfile, mode='w')
    except:
        fd = fs_handle.open_file(logfile, flags='w')

def log(string):
    global fd
    if fd:
        if isinstance(string, string_types):
            fd.write(('{0}: {1}'.format(datetime.datetime.now().isoformat(), string) + '\n').encode())
        else:
            fd.write(('{0}: {1}'.format(datetime.datetime.now().isoformat(),
            'ERROR! Attempting to write a non-string object to logfile') + '\n').encode())

def kill_logger():
    global fd
    if fd:
        try:
            fd.flush()
            fd.close()
        except:
            pass


def create_directories(app_id, run_id, param_string, type, sub_type=None):

    pyhdfs_handle = get()

    if pyhdfs_handle.exists(project_path() + "Experiments"):
        hdfs_events_parent_dir = project_path() + "Experiments"
    elif pyhdfs_handle.exists(project_path() + "Logs"):
        hdfs_events_parent_dir = project_path() + "Logs/TensorFlow"
        try:
            st = hdfs.stat(hdfs_events_parent_dir)
            if not bool(st.st_mode & stat.S_IWGRP):
                hdfs.chmod(hdfs_events_parent_dir, "g+w")
        except IOError:
            # If this happens then the permission is set correct already since the creator of the /Logs/TensorFlow already set group writable
            pass

    hdfs_appid_logdir = hdfs_events_parent_dir + "/" + app_id
    #if not pyhdfs_handle.exists(hdfs_appid_logdir):
    #pyhdfs_handle.create_directory(hdfs_appid_logdir)

    hdfs_run_id_logdir = hdfs_appid_logdir + "/" + type + "/run." + str(run_id)

    if sub_type:
        hdfs_exec_logdir = hdfs_run_id_logdir + "/" + str(sub_type) + '/' + str(param_string)
    elif not param_string and not sub_type:
        hdfs_exec_logdir = hdfs_run_id_logdir + '/'
    else:
        hdfs_exec_logdir = hdfs_run_id_logdir + '/' + str(param_string)

    #Need to remove directory if it exists (might be a task retry)
    if pyhdfs_handle.exists(hdfs_exec_logdir):
        pyhdfs_handle.delete(hdfs_exec_logdir, recursive=True)

    pyhdfs_handle.create_directory(hdfs_exec_logdir)

    logfile = hdfs_exec_logdir + '/' + 'logfile'
    os.environ['EXEC_LOGFILE'] = logfile

    return hdfs_exec_logdir, hdfs_appid_logdir

def copy_to_project(local_path, relative_hdfs_path, overwrite=False, project=project_name()):

    if "PDIR" in os.environ:
        full_local = os.environ['PDIR'] + '/' + local_path
    else:
        full_local = os.getcwd() + '/' + local_path

    proj_path = project_path(project)
    project_hdfs_path = proj_path + '/' + relative_hdfs_path

    if overwrite:
        hdfs_handle = get()
        split = local_path.split('/')
        filename = split[len(split) - 1]
        if filename == '/':
            filename = split[len(split) - 2]
        full_project_path = proj_path + '/' + relative_hdfs_path + '/' + filename
        if hdfs_handle.exists(full_project_path):
            hdfs_handle.delete(full_project_path, recursive=True)

    hdfs.put(full_local, project_hdfs_path)

def copy_from_project(relative_hdfs_path, local_path, overwrite=False, project=project_name()):

    if "PDIR" in os.environ:
        full_local = os.environ['PDIR'] + '/' + local_path
    else:
        full_local = os.getcwd() + '/' + local_path

    proj_path = project_path(project)
    project_hdfs_path = proj_path + '/' + relative_hdfs_path

    if overwrite:
        split = relative_hdfs_path.split('/')
        filename = split[len(split) - 1]
        full_local_path = full_local + '/' + filename
        if os.path.isdir(full_local_path):
            shutil.rmtree(full_local_path)
        elif os.path.isfile(full_local_path):
            os.remove(full_local_path)

    hdfs.get(project_hdfs_path, full_local)

def get_experiments_dir():
    pyhdfs_handle = get()
    if pyhdfs_handle.exists(project_path() + "Experiments"):
        return project_path() + "Experiments"
    elif pyhdfs_handle.exists(project_path() + "Logs"):
        return project_path() + "Logs/TensorFlow"


def _expand_path(hdfs_path, project, exists=True):
    # Check if a full path is supplied. If not, assume it is a relative path for this project - then build its full path and return it.
    if hdfs_path.startsWith("/Projects/"):
        hdfs_path = "hdfs://" + hdfs_path
    elif not hdfs_path.startsWith("hdfs://"):
        # if the file URL type is not HDFS, throw an error
        if "://" in hdfs_path:
            raise IOError("path %s must be a full hdfs path or a relative path" % hdfs_path)    
        proj_path = project_path(project_name())
        hdfs_path = proj_path + '/' + hdfs_path
    if exists == True and not hdfs.path.exists(hdfs_path):
        raise IOError("path %s not found" % hdfs_path)
    return hdfs_path
    

#    
# Globbing gives you the list of files in a dir that matches a supplied pattern    
#    >>> import glob
#    >>> glob.glob('./[0-9].*')
#        ['./1.gif', './2.txt']
# glob is implemented as  os.listdir() and fnmatch.fnmatch()
# We implement glob as hdfs.ls() and fnmatch.filter()
#
def glob(hdfs_path, recursive=False, project=project_name()):
    """ 
    Finds all the pathnames matching a specified pattern according to the rules used by the Unix shell, although results are returned in arbitrary order. 
 

    Args:
     :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to project_name in HDFS.
     :project_name: If the supplied hdfs_path is a relative path, it will look for that file in this project's subdir in HDFS.

    Raises: IOError

    Returns:
      A possibly-empty list of path names that match pathname, which must be a string containing a path specification. pathname can be either absolute
    """
    
    # Get the full path to the dir for the input glob pattern
    # "hdfs://Projects/jim/blah/*.jpg" => "hdfs://Projects/jim/blah"
    # Then, ls on 'hdfs://Projects/jim/blah', then filter out results
    lastSep = hdfs_path.rfind("/")
    inputDir = hdfs_path[:lastSep]
    inputDir = _expand_path(inputDir, project)
    pattern = hdfs_path[lastSep+1:]
    if not hdfs.path.exists(inputDir):
        raise IOError("Glob path %s not found" % inputDir)
    dirContents = hdfs.ls(inputDir, recursive=recursive)
    return fnmatch.filter(dirContents, pattern)


def ls(hdfs_path, recursive=False, project=project_name()):
    """ 
    Returns all the pathnames in the supplied directory.
 

    Args:
     :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to project_name in HDFS).
     :recursive: if it is a directory and recursive is True, the list contains one item for every file or directory in the tree rooted at hdfs_path.
     :project_name: If the supplied hdfs_path is a relative path, it will look for that file in this project's subdir in HDFS.



    Raises: IOError

    Returns:
      A possibly-empty list of path names stored in the supplied path.
    """
    hdfs_path = _expand_path(hdfs_path, project)
    return hdfs.ls(hdfs_path, recursive=recursive)


def lsl(hdfs_path, recursive=False, project=project_name()):
    """ 
    Returns all the pathnames in the supplied directory.
 

    Args:
     :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to project_name in HDFS).
     :recursive: if it is a directory and recursive is True, the list contains one item for every file or directory in the tree rooted at hdfs_path.
     :project_name: If the supplied hdfs_path is a relative path, it will look for that file in this project's subdir in HDFS.



    Raises: IOError

    Returns:
      A possibly-empty list of path names stored in the supplied path.
    """
    hdfs_path = _expand_path(hdfs_path, project)
    return hdfs.lsl(hdfs_path, recursive=recursive)


def rmr(hdfs_path, project=project_name()):
    """ 
    Recursively remove files and directories.
 

    Args:
     :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to project_name in HDFS).
     :project_name: If the supplied hdfs_path is a relative path, it will look for that file in this project's subdir in HDFS.

    Raises: IOError
    """
    hdfs_path = _expand_path(hdfs_path, project)
    return hdfs.rmr(hdfs_path)


def mkdir(hdfs_path, project=project_name()):
    """ 
    Create a directory and its parents as needed.
 

    Args:
     :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to project_name in HDFS).
     :project_name: If the supplied hdfs_path is a relative path, it will look for that file in this project's subdir in HDFS.

    Raises: IOError
    """
    hdfs_path = _expand_path(hdfs_path, project, exists=False)
    return hdfs.mkdir(hdfs_path)

def move(src, dest):
    """ 
    Move or rename src to dest.
 
    Args:
     :src: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
     :dest: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).

    Raises: IOError
    """
    src = _expand_path(src, project_name())
    dest = _expand_path(dest, project_name(), exists=False)    
    return hdfs.move(src, dest)

def rename(src, dest):
    """ 
    Rename src to dest.
 
    Args:
     :src: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
     :dest: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).

    Raises: IOError
    """
    src = _expand_path(src, project_name())
    dest = _expand_path(dest, project_name(), exists=False)
    return hdfs.rename(src, dest)

def chown(hdfs_path, user, group, project=project_name()):
    """ 
    Change file owner and group.

    Args:
     :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to the given project path in HDFS).
     :user: New hdfs username
     :group: New hdfs group
     :project_name: If this value is not specified, it will get the path to your project. If you need to path to another project,
     you can specify the name of the project as a string.

    Raises: IOError
    """
    hdfs_path = _expand_path(hdfs_path, project)
    return hdfs.chown(hdfs_path, user, group)

def chmod(hdfs_path, mode, user, project=project_name()):
    """ 
    Change file mode bits.

    Args:
     :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
     :user: New hdfs username
     :mode: File mode (user/group/world privilege) bits 
     :project_name: If this value is not specified, it will get the path to your project. If you need to path to another project,
     you can specify the name of the project as a string.

    Raises: IOError
    """
    hdfs_path = _expand_path(hdfs_path, project)
    return hdfs.chown(hdfs_path, user, group)

def stat(hdfs_path, project=project_name()):
    """ 
    Performs the equivalent of os.stat() on path, returning a StatResult object.

 

    Args:
     :hdfs_path: If this value is not specified, it will get the path to your project. You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
     :project_name: If this value is not specified, it will get the path to your project. If you need to path to another project,
     you can specify the name of the project as a string.


    Raises: IOError

    Returns:
        StatResult object
    """
    hdfs_path = _expand_path(hdfs_path, project)
    return hdfs.stat(hdfs_path)

def access(hdfs_path, mode, project=project_name()):
    """ 
    Perform the equivalent of os.access() on path.

    Args:
     :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
     :mode: File mode (user/group/world privilege) bits 
     :project_name: If this value is not specified, it will get the path to your project. If you need to path to another project,
     you can specify the name of the project as a string.

    Returns:
        True if access is allowed, False if not.
    Raises: IOError
    """
    hdfs_path = _expand_path(hdfs_path, project)
    return hdfs.access(hdfs_path, mode)

def _mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: 
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def copy_to_local(hdfs_path, project=project_name(), overwrite=False):
    """ 
    Copies a file or a directory (recurisvely) to a local directory indicated by the $PDIR env variable.
    The command recreates the directory structure under hdfs:///Projects/<project_name>/ in the local $PDIR directory.
    For example, if you execute:
        copy_to_local("hdfs:///Projects/demo/Resources/myFile.csv")
    it will create a 'Resources' directory under the $PDIR directory, and copy the myFile.csv file to $PDIR/Resources/myFile.csv
 

    Args:
     :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
     :project_name: If this value is not specified, it will get the path to your project. If you need to path to another project,
     you can specify the name of the project as a string.


    Raises: IOError

    Returns:
        Path to the file/directory on the local disk.
    """

    hdfs_path = _expand_path(hdfs_path, project)
    
    sub_path = hdfs_path.find("hdfs:///Projects/" + project)
    rel_path = hdfs_path[sub_path+1:]

    local_path = os.environ['PDIR'] + rel_path

    pos = local_path.rfind("/")
    local_dir = local_path[:pos]
    _mkdir_p(local_dir)

    if overwrite == True:
        if pydoop.hdfs.path.isdir(hdfs_path) == True :
            shutil.rmtree(local_path)
        else:
            os.remove(local_path)

    pydoop.hdfs.get(hdfs_path, local_dir)

    
def copy_to_hdfs(local_path, hdfs_path, project=project_name()):
    """ 
    Copies a file or a directory (recurisvely) using local path (relative to (under) the path identfied by $PDIR) to a path in hdfs (hdfs_path).
    For example, if you execute:
        copy_from_local("data.tfrecords", "/Resources/", project="demo")
    This will copy the file in $PDIR/data.tfrecords to hdfs://Projects/demo/Resources/data.tfrecords

    Args:
     :local_path: a local directory, relative to (under) the path given by the $PDIR environment variable.
     :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
     :project_name: If this value is not specified, it will get the path to your project. If you need to path to another project,
     you can specify the name of the project as a string.


    Raises: IOError

    """
    
    if not local_path.startsWith(os.environ['PDIR']):
        local_path = os.environ['PDIR'] + "/" + local_path
    hdfs_path = _expand_path(hdfs_path, project)    
    pydoop.hdfs.put(local_dir, hdfs_path)

def open_file(hdfs_file, project=project_name(), flags='rw', buff_size=0):
    """ 
    Opens a file for read/write/append and returns a file descriptor object (fd) that should be closed when no longer needed.

    Args:
     :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
     :flags: Supported opening modes are “r”, “w”, “a”. In addition, a trailing “t” can be added to specify text mode (e.g., “rt” = open for reading text).K
     :buff_size: Pass 0 as buff_size if you want to use the “configured” values, i.e., the ones set in the Hadoop configuration files.


    Returns:
        A file descriptor (fd) that needs to be closed (fd.close()) when it is no longer needed.

    Raises: IOError
            If the file does not exist.
    """
    hdfs_path = _expand_path(hdfs_path, project)
    fs_handle = hdfs.get_fs()
    fd = fs_handle.open_file(hdfs_filek, flags)
    return fd


def exists(hdfs_file, project=project_name()):
    """ 
    Return True if hdfs_path exists in the default HDFS.

    Args:
     :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
     :project_name: If this value is not specified, it will get the path to your project. If you need to path to another project,
     you can specify the name of the project as a string.


    Returns:
        True if hdfs_path exists.

    Raises: IOError
    """
    hdfs_path = _expand_path(hdfs_path, project)
    return hdfs.exists(hdfs_path)



def isdir(hdfs_file, project=project_name()):
    """ 
    Return True if path refers to a directory.

    Args:
     :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
     :project_name: If this value is not specified, it will get the path to your project. If you need to path to another project,
     you can specify the name of the project as a string.


    Returns:
        True if path refers to a directory.

    Raises: IOError
    """
    hdfs_path = _expand_path(hdfs_path, project)
    return hdfs.isdir(hdfs_path)


def isfile(hdfs_file, project=project_name()):
    """ 
    Return True if path refers to a file.

    Args:
     :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
     :project_name: If this value is not specified, it will get the path to your project. If you need to path to another project,
     you can specify the name of the project as a string.


    Returns:
        True if path refers to a file.

    Raises: IOError
    """
    hdfs_path = _expand_path(hdfs_path, project)
    return hdfs.isfile(hdfs_path)


