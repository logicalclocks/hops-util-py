"""
API for interacting with the file system on Hops (HopsFS).

It is a wrapper around pydoop together with utility functions that are Hops-specific.
"""
import pydoop.hdfs as hdfs
import os
import datetime
from six import string_types
import shutil
import stat as local_stat
import fnmatch
import os
import errno

fd = None

def project_user():
    """
    Gets the project username ("project__user") from environment variables

    Returns:
        the project username
    """

    try:
        hops_user = os.environ["HADOOP_USER_NAME"]
    except:
        hops_user = os.environ["HDFS_USER"]
    return hops_user

def project_name():
    """
    Extracts the project name from the project username ("project__user")

    Returns:
        project name
    """
    hops_user = project_user()
    hops_user_split = hops_user.split("__")  # project users have username project__user
    project = hops_user_split[0]
    return project

def project_path(project=None):
    """ Get the path in HopsFS where the HopsWorks project is located. To point to a particular dataset, this path should be
    appended with the name of your dataset.

    >>> from hops import hdfs
    >>> project_path = hdfs.project_path()
    >>> print("Project path: {}".format(project_path))

    Args:
        :project_name: If this value is not specified, it will get the path to your project. If you need to path to another project, you can specify the name of the project as a string.

    Returns:
        returns the project absolute path
    """

    if project:
        # abspath means "hdfs://namenode:port/ is preprended
        return hdfs.path.abspath("/Projects/" + project + "/")
    project = project_name()
    return hdfs.path.abspath("/Projects/" + project + "/")

def get():
    """ Get a handle to pydoop hdfs using the default namenode (specified in hadoop config)

    Returns:
        Pydoop hdfs handle
    """
    return hdfs.hdfs('default', 0, user=project_user())


def get_fs():
    """ Get a handle to pydoop fs using the default namenode (specified in hadoop config)

    Returns:
        Pydoop fs handle
    """
    return hdfs.fs.hdfs('default', 0, user=project_user())


def _expand_path(hdfs_path, project="", exists=True):
    """
    Expands a given path. If the path is /Projects.. hdfs:// is prepended.
    If the path is ../ the full project path is prepended.

    Args:
        :hdfs_path the path to be expanded
        :exists boolean flag, if this is true an exception is thrown if the expanded path does not exist.

    Raises:
        IOError if exists flag is true and the path does not exist

    Returns:
        path expanded with HDFS and project
    """
    if project == "":
        project = project_name()
    # Check if a full path is supplied. If not, assume it is a relative path for this project - then build its full path and return it.
    if hdfs_path.startswith("/Projects/") or hdfs_path.startswith("/Projects"):
        hdfs_path = "hdfs://" + hdfs_path
    elif not hdfs_path.startswith("hdfs://"):
        # if the file URL type is not HDFS, throw an error
        if "://" in hdfs_path:
            raise IOError("path %s must be a full hdfs path or a relative path" % hdfs_path)
        proj_path = project_path(project)
        hdfs_path = proj_path + hdfs_path
    if exists == True and not hdfs.path.exists(hdfs_path):
        raise IOError("path %s not found" % hdfs_path)
    return hdfs_path


def _init_logger():
    """
    Initialize the logger by opening the log file and pointing the global fd to the open file
    """
    logfile = os.environ['EXEC_LOGFILE']
    fs_handle = get_fs()
    global fd
    try:
        fd = fs_handle.open_file(logfile, mode='w')
    except:
        fd = fs_handle.open_file(logfile, flags='w')


def log(string):
    """
    Logs a string to the log file

    Args:
        :string: string to log
    """
    global fd
    if fd:
        if isinstance(string, string_types):
            fd.write(('{0}: {1}'.format(datetime.datetime.now().isoformat(), string) + '\n').encode())
        else:
            fd.write(('{0}: {1}'.format(datetime.datetime.now().isoformat(),
                                        'ERROR! Attempting to write a non-string object to logfile') + '\n').encode())


def _kill_logger():
    """
    Closes the logfile
    """
    global fd
    if fd:
        try:
            fd.flush()
            fd.close()
        except:
            pass


def _create_directories(app_id, run_id, param_string, type, sub_type=None):
    """
    Creates directories for an experiment, if Experiments folder exists it will create directories
    below it, otherwise it will create them in the Logs directory.

    Args:
        :app_id: YARN application ID of the experiment
        :run_id: Experiment ID
        :param_string: name of the new directory created under parent directories
        :type: type of the new directory parent, e.g differential_evolution
        :sub_type: type of sub directory to parent, e.g generation

    Returns:
        The new directories for the yarn-application and for the execution (hdfs_exec_logdir, hdfs_appid_logdir)
    """

    pyhdfs_handle = get()

    if pyhdfs_handle.exists(project_path() + "Experiments"):
        hdfs_events_parent_dir = project_path() + "Experiments"
    elif pyhdfs_handle.exists(project_path() + "Logs"):
        hdfs_events_parent_dir = project_path() + "Logs/TensorFlow"
        try:
            st = hdfs.stat(hdfs_events_parent_dir)
            if not bool(st.st_mode & local_stat.S_IWGRP):  # if not group writable make it so
                hdfs.chmod(hdfs_events_parent_dir, "g+w")
        except IOError:
            # If this happens then the permission is set correct already since the creator of the /Logs/TensorFlow already set group writable
            pass

    hdfs_appid_logdir = hdfs_events_parent_dir + "/" + app_id
    # if not pyhdfs_handle.exists(hdfs_appid_logdir):
    # pyhdfs_handle.create_directory(hdfs_appid_logdir)

    hdfs_run_id_logdir = hdfs_appid_logdir + "/" + type + "/run." + str(run_id)

    # determine directory structure based on arguments
    if sub_type:
        hdfs_exec_logdir = hdfs_run_id_logdir + "/" + str(sub_type) + '/' + str(param_string)
    elif not param_string and not sub_type:
        hdfs_exec_logdir = hdfs_run_id_logdir + '/'
    else:
        hdfs_exec_logdir = hdfs_run_id_logdir + '/' + str(param_string)

    # Need to remove directory if it exists (might be a task retry)
    if pyhdfs_handle.exists(hdfs_exec_logdir):
        pyhdfs_handle.delete(hdfs_exec_logdir, recursive=True)

    # create the new directory
    pyhdfs_handle.create_directory(hdfs_exec_logdir)

    # update logfile
    logfile = hdfs_exec_logdir + '/' + 'logfile'
    os.environ['EXEC_LOGFILE'] = logfile

    return hdfs_exec_logdir, hdfs_appid_logdir


def copy_to_hdfs(local_path, relative_hdfs_path, overwrite=False, project=None):
    """
    Copies a path from local filesystem to HDFS project (recursively) using relative path in $CWD to a path in hdfs (hdfs_path)

    For example, if you execute:

    >>> copy_to_hdfs("data.tfrecords", "/Resources/", project="demo")

    This will copy the file data.tfrecords to hdfs://Projects/demo/Resources/data.tfrecords

    Args:
        :local_path: the path on the local filesystem to copy
        :relative_hdfs_path: a path in HDFS relative to the project root to where the local path should be written
        :overwrite: a boolean flag whether to overwrite if the path already exists in HDFS
        :project: name of the project, defaults to the current HDFS user's project
    """
    if project == None:
        project = project_name()

    if "PDIR" in os.environ:
        full_local = os.environ['PDIR'] + '/' + local_path
    else:
        full_local = os.getcwd() + '/' + local_path

    hdfs_path = _expand_path(relative_hdfs_path, project)

    if overwrite:
        hdfs_handle = get()
        split = local_path.split('/')
        filename = split[len(split) - 1]
        if filename == '/':
            filename = split[len(split) - 2]
        full_project_path = hdfs_path + '/' + filename

        # check if project path exist, if so delete it (since overwrite flag was set to true)
        if hdfs_handle.exists(full_project_path):
            hdfs_handle.delete(full_project_path, recursive=True)

    # copy directories from local path to HDFS project path
    hdfs.put(full_local, hdfs_path)


def copy_to_local(hdfs_path, local_path, overwrite=False, project=None):
    """
    Copies a path from HDFS project to local filesystem. If there is not enough space on the local scratch directory, an exception is thrown.

    Raises:
      IOError if there is not enough space to localize the file/directory in HDFS to the scratch directory ($PDIR)

    Args:
        :local_path: the path on the local filesystem to copy
        :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
        :overwrite: a boolean flag whether to overwrite if the path already exists in HDFS
        :project: name of the project, defaults to the current HDFS user's project

    Returns:
        the full local pathname of the file/dir
    """
    import os
    import pydoop.hdfs.path as path
    
    if project == None:
        project = project_name()

    if "PDIR" in os.environ:
        full_local = os.environ['PDIR'] + '/' + local_path
    else:
        full_local = os.getcwd() + '/' + local_path
        
    project_hdfs_path = _expand_path(hdfs_path, project=project)
    sub_path = hdfs_path.find("hdfs:///Projects/" + project)
    rel_path = hdfs_path[sub_path + 1:]

    # Get the amount of free space on the local drive
    stat = os.statvfs(full_local)
    free_space_bytes = stat.f_bsize * stat.f_bavail    

    hdfs_size = path.getsize(project_hdfs_path)

    if (hdfs_size > free_space_bytes):
        raise IOError("Not enough local free space available on scratch directory: %s" % path)        
    
    if overwrite:
        split = rel_path.split('/')
        filename = split[len(split) - 1]
        full_local_path = full_local + '/' + filename
        if os.path.isdir(full_local_path):
            shutil.rmtree(full_local_path)
        elif os.path.isfile(full_local_path):
            os.remove(full_local_path)

    hdfs.get(project_hdfs_path, full_local)

    return full_local

def cp(src_hdfs_path, dest_hdfs_path):
    """
    Copy the contents of src_hdfs_path to dest_hdfs_path.

    If src_hdfs_path is a directory, its contents will be copied recursively. Source file(s) are opened for reading and copies are opened for writing.

    Args:
        :src_hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
        :dest_hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).

    """
    src_hdfs_path = _expand_path(src_hdfs_path)
    dest_hdfs_path = _expand_path(dest_hdfs_path)
    hdfs.cp(src_hdfs_path, dest_hdfs_path)


def _get_experiments_dir():
    """
    Gets the folder where the experiments are writing their results

    Returns:
        the folder where the experiments are writing results
    """
    pyhdfs_handle = get()
    if pyhdfs_handle.exists(project_path() + "Experiments"):
        return project_path() + "Experiments"
    elif pyhdfs_handle.exists(project_path() + "Logs"):
        return project_path() + "Logs/TensorFlow"


def glob(hdfs_path, recursive=False, project=None):
    """ 
    Finds all the pathnames matching a specified pattern according to the rules used by the Unix shell, although results are returned in arbitrary order.

    Globbing gives you the list of files in a dir that matches a supplied pattern

    >>> import glob
    >>> glob.glob('./[0-9].*')
    >>> ['./1.gif', './2.txt']

    glob is implemented as  os.listdir() and fnmatch.fnmatch()
    We implement glob as hdfs.ls() and fnmatch.filter()

    Args:
     :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to project_name in HDFS.
     :project: If the supplied hdfs_path is a relative path, it will look for that file in this project's subdir in HDFS.

    Raises:
        IOError if the supplied hdfs path does not exist

    Returns:
      A possibly-empty list of path names that match pathname, which must be a string containing a path specification. pathname can be either absolute
    """

    # Get the full path to the dir for the input glob pattern
    # "hdfs://Projects/jim/blah/*.jpg" => "hdfs://Projects/jim/blah"
    # Then, ls on 'hdfs://Projects/jim/blah', then filter out results
    if project == None:
        project = project_name()
    lastSep = hdfs_path.rfind("/")
    inputDir = hdfs_path[:lastSep]
    inputDir = _expand_path(inputDir, project)
    pattern = hdfs_path[lastSep + 1:]
    if not hdfs.path.exists(inputDir):
        raise IOError("Glob path %s not found" % inputDir)
    dirContents = hdfs.ls(inputDir, recursive=recursive)
    return fnmatch.filter(dirContents, pattern)


def ls(hdfs_path, recursive=False, project=None):
    """ 
    Returns all the pathnames in the supplied directory.

    Args:
        :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to project_name in HDFS).
        :recursive: if it is a directory and recursive is True, the list contains one item for every file or directory in the tree rooted at hdfs_path.
        :project: If the supplied hdfs_path is a relative path, it will look for that file in this project's subdir in HDFS.

    Returns:
      A possibly-empty list of path names stored in the supplied path.
    """
    if project == None:
        project = project_name()
    hdfs_path = _expand_path(hdfs_path, project)
    return hdfs.ls(hdfs_path, recursive=recursive)


def lsl(hdfs_path, recursive=False, project=None):
    """ 
    Returns all the pathnames in the supplied directory.

    Args:
        :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to project_name in HDFS).
        :recursive: if it is a directory and recursive is True, the list contains one item for every file or directory in the tree rooted at hdfs_path.
        :project: If the supplied hdfs_path is a relative path, it will look for that file in this project's subdir in HDFS.

    Returns:
        A possibly-empty list of path names stored in the supplied path.
    """
    if project == None:
        project = project_name()
    hdfs_path = _expand_path(hdfs_path, project)
    return hdfs.lsl(hdfs_path, recursive=recursive)


def rmr(hdfs_path, project=None):
    """ 
    Recursively remove files and directories.

    Args:
        :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to project_name in HDFS).
        :project: If the supplied hdfs_path is a relative path, it will look for that file in this project's subdir in HDFS.

    """
    if project == None:
        project = project_name()
    hdfs_path = _expand_path(hdfs_path, project)
    return hdfs.rmr(hdfs_path)


def mkdir(hdfs_path, project=None):
    """ 
    Create a directory and its parents as needed.

    Args:
        :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to project_name in HDFS).
        :project: If the supplied hdfs_path is a relative path, it will look for that file in this project's subdir in HDFS.

    """
    if project == None:
        project = project_name()
    hdfs_path = _expand_path(hdfs_path, project, exists=False)
    return hdfs.mkdir(hdfs_path)


def move(src, dest):
    """ 
    Move or rename src to dest.
 
    Args:
        :src: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
        :dest: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).

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
    """
    src = _expand_path(src, project_name())
    dest = _expand_path(dest, project_name(), exists=False)
    return hdfs.rename(src, dest)


def chown(hdfs_path, user, group, project=None):
    """ 
    Change file owner and group.

    Args:
        :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to the given project path in HDFS).
        :user: New hdfs username
        :group: New hdfs group
        :project: If this value is not specified, it will get the path to your project. If you need to path to another project, you can specify the name of the project as a string.
    """
    if project == None:
        project = project_name()
    hdfs_path = _expand_path(hdfs_path, project)
    return hdfs.chown(hdfs_path, user, group)


def chmod(hdfs_path, mode, project=None):
    """ 
    Change file mode bits.

    Args:
        :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
        :mode: File mode (user/group/world privilege) bits
        :project: If this value is not specified, it will get the path to your project. If you need to path to another project, you can specify the name of the project as a string.
    """
    if project == None:
        project = project_name()
    hdfs_path = _expand_path(hdfs_path, project)
    return hdfs.chmod(hdfs_path, mode)


def stat(hdfs_path, project=None):
    """ 
    Performs the equivalent of os.stat() on path, returning a StatResult object.

    Args:
        :hdfs_path: If this value is not specified, it will get the path to your project. You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
        :project: If this value is not specified, it will get the path to your project. If you need to path to another project, you can specify the name of the project as a string.

    Returns:
        StatResult object
    """
    if project == None:
        project = project_name()
    hdfs_path = _expand_path(hdfs_path, project)
    return hdfs.stat(hdfs_path)


def access(hdfs_path, mode, project=None):
    """ 
    Perform the equivalent of os.access() on path.

    Args:
        :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
        :mode: File mode (user/group/world privilege) bits
        :project: If this value is not specified, it will get the path to your project. If you need to path to another project, you can specify the name of the project as a string.

    Returns:
        True if access is allowed, False if not.
    """
    if project == None:
        project = project_name()
    hdfs_path = _expand_path(hdfs_path, project)
    return hdfs.access(hdfs_path, mode)


def _mkdir_p(path):
    """
    Creates path on local filesystem

    Args:
        path to create

    Raises:
        OSError
    """
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def open_file(hdfs_path, project=None, flags='rw', buff_size=0):
    """
    Opens an HDFS file for read/write/append and returns a file descriptor object (fd) that should be closed when no longer needed.

    Args:
        hdfs_path: you can specify either a full hdfs pathname or a relative one (relative to your project's path in HDFS)
        flags: supported opening modes are 'r', 'w', 'a'. In addition, a trailing 't' can be added to specify text mode (e.g, 'rt' = open for reading text)
        buff_size: Pass 0 as buff_size if you want to use the "configured" values, i.e the ones set in the Hadoop configuration files.

    Returns:
        A file descriptor (fd) that needs to be closed (fd-close()) when it is no longer needed.

    Raises:
        IOError: If the file does not exist.
    """
    if project == None:
        project = project_name()
    hdfs_path = _expand_path(hdfs_path, project, exists=False)
    fs_handle = get_fs()
    fd = fs_handle.open_file(hdfs_path, flags, buff_size=buff_size)
    return fd


def close():
    """
    Closes an the HDFS connection (disconnects to the namenode)
    """
    hdfs.close()


def exists(hdfs_path, project=None):
    """ 
    Return True if hdfs_path exists in the default HDFS.

    Args:
        :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
        :project: If this value is not specified, it will get the path to your project. If you need to path to another project, you can specify the name of the project as a string.


    Returns:
        True if hdfs_path exists.

    Raises: IOError
    """
    if project == None:
        project = project_name()

    try:
        hdfs_path = _expand_path(hdfs_path, project)
    except IOError:
        return False
    return hdfs.path.exists(hdfs_path)


def isdir(hdfs_path, project=None):
    """ 
    Return True if path refers to a directory.

    Args:
        :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
        :project: If this value is not specified, it will get the path to your project. If you need to path to another project, you can specify the name of the project as a string.

    Returns:
        True if path refers to a directory.

    Raises: IOError
    """
    if project == None:
        project = project_name()
    hdfs_path = _expand_path(hdfs_path, project)
    return hdfs.isdir(hdfs_path)


def isfile(hdfs_path, project=None):
    """ 
    Return True if path refers to a file.

    Args:
        :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
        :project: If this value is not specified, it will get the path to your project. If you need to path to another project, you can specify the name of the project as a string.

    Returns:
        True if path refers to a file.

    Raises: IOError
    """
    if project == None:
        project = project_name()
    hdfs_path = _expand_path(hdfs_path, project)
    return hdfs.isfile(hdfs_path)


def capacity():
    """
    Returns the raw capacity of the filesystem

    Returns:
        filesystem capacity (int)
    """
    return hdfs.capacity()


def dump(data, hdfs_path):
    """
    Dumps data to a file

    Args:
        :data: data to write to hdfs_path
        :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
    """

    #split = hdfs_path.split('/')
    #filename = split[len(split) - 1]
    #directory = "/".join(split[0:len(split)-1])
    hdfs_path = _expand_path(hdfs_path, exists=False)
    return hdfs.dump(data, hdfs_path)


def load(hdfs_path):
    """
    Read the content of hdfs_path and return it.

    Args:
        :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).

    Returns:
        the read contents of hdfs_path
    """
    hdfs_path = _expand_path(hdfs_path)
    return hdfs.load(hdfs_path)

def ls(hdfs_path, recursive=False):
    """
    lists a directory in HDFS

    Args:
        :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).

    Returns:
        returns a list of hdfs paths
    """
    hdfs_path = _expand_path(hdfs_path)
    return hdfs.ls(hdfs_path, recursive=recursive)

def stat(hdfs_path):
    """
    Performs the equivalent of os.stat() on hdfs_path, returning a StatResult object.

    Args:
        :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).

    Returns:
        returns a list of hdfs paths
    """
    hdfs_path = _expand_path(hdfs_path)
    return hdfs.stat(hdfs_path)

def abs_path(hdfs_path):
    """
     Return an absolute path for hdfs_path.

     Args:
         :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).

    Returns:
        Return an absolute path for hdfs_path.
    """
    return _expand_path(hdfs_path)

def localize(hdfs_path):
    """
     Localizes (copies) the given file or directory from HDFS into a local scratch directory, indicated by the env variable $PDIR.
     Returns the absolute path for the local file. If there is not enough space on the local scratch directory, an exception is thrown.

     Args:
         :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).

     Raises:
        IOError if there is not enough space to localize the file/directory in HDFS to the scratch directory ($PDIR)

     Returns:
        Return an absolute path for local file/directory.
    """

    return copy_to_local(hdfs_path, "", overwrite=True)

