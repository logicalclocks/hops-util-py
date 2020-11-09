"""
API for interacting with the file system on Hops (HopsFS).

It is a wrapper around pydoop together with utility functions that are Hops-specific.
"""
import socket
from six import string_types
import shutil
import fnmatch
import os
import errno

from hops import constants
from hops.service_discovery import ServiceDiscovery
import sys
import subprocess

import ntpath
import importlib

# Compatibility with SageMaker
pydoop_available = True
try:
    import pydoop.hdfs as hdfs
    import pydoop.hdfs.path as path
    import pydoop.hdfs.fs as hdfs_fs
except:
    pydoop_available = False

import re
from xml.dom import minidom

tls_enabled = None
webhdfs_address = None

if pydoop_available: 
    # Replace Pydoop split method to be able to support hopsfs:// schemes
    class _HopsFSPathSplitter(hdfs.path._HdfsPathSplitter):
        @classmethod
        def split(cls, hdfs_path, user):
            if not hdfs_path:
                cls.raise_bad_path(hdfs_path, "empty")
            scheme, netloc, path = cls.parse(hdfs_path)
            if not scheme:
                scheme = "file" if hdfs_fs.default_is_local() else "hdfs"
            if scheme == "hdfs" or scheme == "hopsfs":
                if not path:
                    cls.raise_bad_path(hdfs_path, "path part is empty")
                if ":" in path:
                    cls.raise_bad_path(
                        hdfs_path, "':' not allowed outside netloc part"
                    )
                hostname, port = cls.split_netloc(netloc)
                if not path.startswith("/"):
                    path = "/user/%s/%s" % (user, path)
            elif scheme == "file":
                hostname, port, path = "", 0, netloc + path
            else:
                cls.raise_bad_path(hdfs_path, "unsupported scheme %r" % scheme)
            return hostname, port, path

    hdfs.path._HdfsPathSplitter = _HopsFSPathSplitter

def get_plain_path(abs_path):
    """
    Convert absolute HDFS/HOPSFS path to plain path (dropping prefix and ip)

    Example use-case:

    >>> hdfs.get_plain_path("hdfs://10.0.2.15:8020/Projects/demo_deep_learning_admin000/Models/")
    >>> # returns: "/Projects/demo_deep_learning_admin000/Models/"

     Args:
         :abs_path: the absolute HDFS/hopsfs path containing prefix and/or ip

    Returns:
          the plain path without prefix and ip
    """
    return path.split(path.abspath(abs_path))[2]

def project_id():
    """
    Get the Hopsworks project id from environment variables

    Returns: the Hopsworks project id

    """
    return os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_ID_ENV_VAR]

def project_user():
    """
    Gets the project username ("project__user") from environment variables

    Returns:
        the project username
    """

    try:
        hops_user = os.environ[constants.ENV_VARIABLES.HADOOP_USER_NAME_ENV_VAR]
    except:
        hops_user = os.environ[constants.ENV_VARIABLES.HDFS_USER_ENV_VAR]
    return hops_user

def project_name():
    """
    Extracts the project name from the project username ("project__user") or from the environment if available

    Returns:
        project name
    """
    try:
        return os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_NAME_ENV_VAR]
    except:
        pass

    hops_user = project_user()
    hops_user_split = hops_user.split("__")  # project users have username project__user
    project = hops_user_split[0]
    return project

def project_path(project=None, exclude_nn_addr=False):
    """ Get the path in HopsFS where the HopsWorks project is located. To point to a particular dataset, this path should be
    appended with the name of your dataset.

    >>> from hops import hdfs
    >>> project_path = hdfs.project_path()
    >>> print("Project path: {}".format(project_path))

    Args:
        :project: If this value is not specified, it will get the path to your project. If you need to path to another project, you can specify the name of the project as a string.

    Returns:
        returns the project absolute path
    """

    if project is None:
        project = project_name()

    # abspath means "hdfs://namenode:port/ is preprended
    abspath = hdfs.path.abspath("/Projects/" + project + "/")
    if exclude_nn_addr:
        abspath = re.sub(r"\d+.\d+.\d+.\d+:\d+", "", abspath)
    return abspath


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
    if not isinstance(hdfs_path, string_types):
        hdfs_path = hdfs_path.decode()
    if project == "":
        project = project_name()
    # Check if a full path is supplied. If not, assume it is a relative path for this project - then build its full path and return it.
    if hdfs_path.startswith("/Projects/") or hdfs_path.startswith("/Projects"):
        hdfs_path = "hdfs://" + hdfs_path
    elif not (hdfs_path.startswith("hdfs://") or hdfs_path.startswith("hopsfs://")):
        # if the file URL type is not HDFS, throw an error
        if "://" in hdfs_path:
            raise IOError("path %s must be a full hopsfs path or a relative path" % hdfs_path)
        proj_path = project_path(project)
        hdfs_path = proj_path + hdfs_path
    if exists == True and not hdfs.path.exists(hdfs_path):
        raise IOError("path %s not found" % hdfs_path)
    return hdfs_path

def copy_to_hdfs(local_path, relative_hdfs_path, overwrite=False, project=None):
    """
    Copies a path from local filesystem to HDFS project (recursively) using relative path in $CWD to a path in hdfs (hdfs_path)

    For example, if you execute:

    >>> copy_to_hdfs("data.tfrecords", "/Resources", project="demo")

    This will copy the file data.tfrecords to hdfs://Projects/demo/Resources/data.tfrecords

    Args:
        :local_path: Absolute or local path on the local filesystem to copy
        :relative_hdfs_path: a path in HDFS relative to the project root to where the local path should be written
        :overwrite: a boolean flag whether to overwrite if the path already exists in HDFS
        :project: name of the project, defaults to the current HDFS user's project
    """
    if project == None:
        project = project_name()

    # Absolute path
    if local_path.startswith(os.getcwd()):
        full_local = local_path
    else:
        full_local = os.getcwd() + '/' + local_path

    hdfs_path = _expand_path(relative_hdfs_path, project, exists=False)

    if overwrite:
        hdfs_path = hdfs_path + "/" + os.path.basename(full_local)
        if exists(hdfs_path):
            # delete hdfs path since overwrite flag was set to true
            delete(hdfs_path, recursive=True)

    print("Started copying local path {} to hdfs path {}\n".format(local_path, hdfs_path))

    # copy directories from local path to HDFS project path
    hdfs.put(full_local, hdfs_path)

    print("Finished copying\n")


def delete(hdfs_path, recursive=False):
    """
    Deletes path, path can be absolute or relative.
    If recursive is set to True and path is a directory, then files will be deleted recursively.

    For example

    >>> delete("/Resources/", recursive=True)

    will delete all files recursively in the folder "Resources" inside the current project.

    Args:
        :hdfs_path: the path to delete (project-relative or absolute)

    Returns:
        None

    Raises:
        IOError when recursive is False and directory is non-empty
    """
    hdfs_path = _expand_path(hdfs_path)
    hdfs_handle = get()
    if hdfs_handle.exists(hdfs_path):
        hdfs_handle.delete(hdfs_path, recursive=recursive)


def copy_to_local(hdfs_path, local_path="", overwrite=False, project=None):
    """
    Copies a directory or file from a HDFS project to a local private scratch directory. If there is not enough space on the local scratch directory, an exception is thrown.
    If the local file exists, and the hdfs file and the local file are the same size in bytes, return 'ok' immediately.
    If the local directory tree exists, and the hdfs subdirectory and the local subdirectory have the same files and directories, return 'ok' immediately.

    For example, if you execute:

    >>> copy_to_local("Resources/my_data")

    This will copy the directory my_data from the Resources dataset in your project to the current working directory on the path ./my_data

    Raises:
      IOError if there is not enough space to localize the file/directory in HDFS to the scratch directory ($PDIR)

    Args:
        :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
        :local_path: the relative or full path to a directory on the local filesystem to copy to (relative to a scratch directory $PDIR), defaults to $CWD
        :overwrite: a boolean flag whether to overwrite if the path already exists in the local scratch directory.
        :project: name of the project, defaults to the current HDFS user's project

    Returns:
        the full local pathname of the file/dir
    """

    if project == None:
        project = project_name()

    if local_path.startswith(os.getcwd()):
        local_dir = local_path
    else:
        local_dir = os.getcwd() + '/' + local_path

    if not os.path.isdir(local_dir):
        raise IOError("You need to supply the path to a local directory. This is not a local dir: %s" % local_dir)

    filename = path.basename(hdfs_path)
    full_local = local_dir + "/" + filename

    project_hdfs_path = _expand_path(hdfs_path, project=project)

    # Get the amount of free space on the local drive
    stat = os.statvfs(local_dir)
    free_space_bytes = stat.f_bsize * stat.f_bavail

    hdfs_size = path.getsize(project_hdfs_path)

    if os.path.isfile(full_local) and not overwrite:
        sz = os.path.getsize(full_local)
        if hdfs_size == sz:
            print("File " + project_hdfs_path + " is already localized, skipping download...")
            return full_local
        else:
            os.remove(full_local)

    if os.path.isdir(full_local) and not overwrite:
        try:
            localized = _is_same_directory(full_local, project_hdfs_path)
            if localized:
                print("Full directory subtree already on local disk and unchanged. Set overwrite=True to force download")
                return full_local
            else:
                shutil.rmtree(full_local)
        except Exception as e:
            print("Failed while checking directory structure to avoid re-downloading dataset, falling back to downloading")
            print(e)
            shutil.rmtree(full_local)

    if hdfs_size > free_space_bytes:
        raise IOError("Not enough local free space available on scratch directory: %s" % local_path)

    if overwrite:
        if os.path.isdir(full_local):
            shutil.rmtree(full_local)
        elif os.path.isfile(full_local):
            os.remove(full_local)

    print("Started copying " + project_hdfs_path + " to local disk on path " + local_dir + "\n")

    hdfs.get(project_hdfs_path, local_dir)

    print("Finished copying\n")

    return full_local


def _is_same_directory(local_path, hdfs_path):
    """
    Validates that the same occurrence and names of files exists in both hdfs and local
    """
    local_file_list = []
    for root, dirnames, filenames in os.walk(local_path):
        for filename in fnmatch.filter(filenames, '*'):
            local_file_list.append(filename)
        for dirname in fnmatch.filter(dirnames, '*'):
            local_file_list.append(dirname)
    local_file_list.sort()

    hdfs_file_list = glob(hdfs_path + '/*', recursive=True)
    hdfs_file_list = [path.basename(str(r)) for r in hdfs_file_list]
    hdfs_file_list.sort()

    if local_file_list == hdfs_file_list:
        return True
    else:
        return False

def cp(src_hdfs_path, dest_hdfs_path, overwrite=False):
    """
    Copy the contents of src_hdfs_path to dest_hdfs_path.

    If src_hdfs_path is a directory, its contents will be copied recursively.
    Source file(s) are opened for reading and copies are opened for writing.

    Args:
        :src_hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
        :dest_hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
        :overwrite: boolean flag whether to overwrite destination path or not.

    """
    src_hdfs_path = _expand_path(src_hdfs_path)
    dest_hdfs_path = _expand_path(dest_hdfs_path, exists=False)

    if overwrite and exists(dest_hdfs_path):
        # delete path since overwrite flag was set to true
        delete(dest_hdfs_path, recursive=True)

    hdfs.cp(src_hdfs_path, dest_hdfs_path)

def glob(hdfs_path, recursive=False, project=None):
    """
    Finds all the pathnames matching a specified pattern according to the rules used by the Unix shell, although results are returned in arbitrary order.

    Globbing gives you the list of files in a dir that matches a supplied pattern

    >>> glob('Resources/*.json')
    >>> ['Resources/1.json', 'Resources/2.json']

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
    return path.isfile(hdfs_path)

def isdir(hdfs_path, project=None):
    """
    Return True if path refers to a directory.

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
    return path.isdir(hdfs_path)


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

def ls(hdfs_path, recursive=False, exclude_nn_addr=False):
    """
    lists a directory in HDFS

    Args:
        :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).

    Returns:
        returns a list of hdfs paths
    """
    if exclude_nn_addr:
        hdfs_path = re.sub(r"\d+.\d+.\d+.\d+:\d+", "", hdfs_path)
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

def add_module(hdfs_path, project=None):
    """
     Add a .py or .ipynb file from HDFS to sys.path

     For example, if you execute:

     >>> add_module("Resources/my_module.py")
     >>> add_module("Resources/my_notebook.ipynb")

     You can import it simply as:

     >>> import my_module
     >>> import my_notebook

     Args:
         :hdfs_path: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS) to a .py or .ipynb file

     Returns:
        Return full local path to localized python file or converted python file in case of .ipynb file
    """

    localized_deps = os.getcwd() + "/localized_deps"
    if not os.path.exists(localized_deps):
        os.mkdir(localized_deps)
        open(localized_deps + '/__init__.py', mode='w').close()

    if localized_deps not in sys.path:
        sys.path.append(localized_deps)

    if project == None:
        project = project_name()
    hdfs_path = _expand_path(hdfs_path, project)

    if path.isfile(hdfs_path) and hdfs_path.endswith('.py'):
        py_path = copy_to_local(hdfs_path, localized_deps, overwrite=True)
        if py_path not in sys.path:
            sys.path.append(py_path)
        _reload(py_path)
        return py_path
    elif path.isfile(hdfs_path) and hdfs_path.endswith('.ipynb'):
        ipynb_path = copy_to_local(hdfs_path, localized_deps, overwrite=True)
        python_path = sys.executable
        jupyter_binary = os.path.dirname(python_path) + '/jupyter'
        if not os.path.exists(jupyter_binary):
            raise Exception('Could not find jupyter binary on path {}'.format(jupyter_binary))

        converted_py_path = os.path.splitext(ipynb_path)[0] + '.py'
        if os.path.exists(converted_py_path):
            os.remove(converted_py_path)

        conversion = subprocess.Popen([jupyter_binary, 'nbconvert', '--to', 'python', ipynb_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = conversion.communicate()
        if conversion.returncode != 0:
            raise Exception("Notebook conversion to .py failed: stdout: {} \n stderr: {}".format(out, err))

        if not os.path.exists(converted_py_path):
            raise Exception('Could not find converted .py file on path {}'.format(converted_py_path))
        if converted_py_path not in sys.path:
            sys.path.append(converted_py_path)
        _reload(converted_py_path)
        return converted_py_path
    else:
        raise Exception("Given path " + hdfs_path + " does not point to a .py or .ipynb file")

def _reload(path):
    try:
        module_name = ntpath.basename(path).split(".")[0]
        imported_module = importlib.import_module(module_name)
        importlib.reload(imported_module)
    except Exception as err:
        print('Failed to automatically reload module on path {} with exception: {}'.format(path, err))

def is_tls_enabled():
    """
    Reads the ipc.server.ssl.enabled property from core-site.xml.

    Returns:
        returns True if ipc.server.ssl.enabled is true. False otherwise.
    """
    global tls_enabled
    if tls_enabled is None:
        hadoop_conf_path = os.environ['HADOOP_CONF_DIR']
        xmldoc = minidom.parse(os.path.join(hadoop_conf_path,'core-site.xml'))
        itemlist = xmldoc.getElementsByTagName('property')
        for item in itemlist:
            name = item.getElementsByTagName("name")[0]
            if name.firstChild.data == "ipc.server.ssl.enabled":
                tls_enabled = item.getElementsByTagName("value")[0].firstChild.data == 'true'
    return tls_enabled

def _get_webhdfs_address():
    """
    Makes an SRV DNS query to get the target and port of NameNode's web interface

    Returns:
        returns webhdfs endpoint
    """
    global webhdfs_address
    if webhdfs_address is None:
        _, port = ServiceDiscovery.get_any_service('http.namenode')
        webhdfs_address = ServiceDiscovery.construct_service_fqdn('http.namenode') + ":" + str(port)
    return webhdfs_address
        
def get_webhdfs_host():
    """
    Makes an SRV DNS query and gets the actual hostname of the NameNode

    Returns:
        returns NameNode's hostname
    """
    return ServiceDiscovery.construct_service_fqdn('http.namenode')

def get_webhdfs_port():
    """
    Makes an SRV DNS query and gets NameNode's port for WebHDFS

    Returns:
        returns NameNode's port for WebHDFS
    """
    return _get_webhdfs_address().split(":")[1]
