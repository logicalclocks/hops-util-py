"""
API for reading/writing numpy arrays to/from HDFS
"""
import hops.hdfs as hdfs
import numpy as np
import os

def load(hdfs_filename, **kwds):
    """
    Reads a file from HDFS into a Numpy Array

     Args:
       :hdfs_filename: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
       :**kwds: You can add any additional args found in numpy.load(...) 

     Returns:
      A numpy array

     Raises:
      IOError: If the file does not exist
    """
    local_path = _copyHdfsToLocalOverwrite(hdfs_filename)
    return np.load(local_path, **kwds)


def loadtxt(hdfs_filename, **kwds):
    """
    Load data from a text file in HDFS into a Numpy Array. 
    Each row in the text file must have the same number of values.

     Args:
       :hdfs_filename: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
       :**kwds: You can add any additional args found in numpy.loadtxt(...) 

     Returns:
      A numpy array

     Raises:
      IOError: If the file does not exist
    """
    local_path = _copyHdfsToLocalOverwrite(hdfs_filename)
    return np.loadtxt(local_path, **kwds)

def genfromtxt(hdfs_filename, **kwds):
    """
    Load data from a HDFS text file, with missing values handled as specified.
    Each line past the first skip_header lines is split at the delimiter character, and characters following the comments character are discarded.

     Args:
       :hdfs_filename: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
       :**kwds: You can add any additional args found in numpy.loadtxt(...) 

     Returns:
      A numpy array

     Raises:
      IOError: If the file does not exist
    """
    local_path = _copyHdfsToLocalOverwrite(hdfs_filename)
    return np.genfromtxt(local_path, **kwds)

def fromregex(hdfs_filename, **kwds):
    """
    Construct an array from a text file, using regular expression parsing.

     Args:
       :hdfs_filename: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
       :**kwds: You can add any additional args found in numpy.loadtxt(...) 

     Returns:
      A numpy array

     Raises:
      IOError: If the file does not exist
    """
    local_path = _copyHdfsToLocalOverwrite(hdfs_filename)
    return np.fromregex(local_path, **kwds)

def fromfile(hdfs_filename, **kwds):
    """
    Construct an array from data in a text or binary file.

     Args:
       :hdfs_filename: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
       :**kwds: You can add any additional args found in numpy.loadtxt(...) 

     Returns:
      A numpy array

     Raises:
      IOError: If the file does not exist
    """
    local_path = _copyHdfsToLocalOverwrite(hdfs_filename)
    return np.fromregex(local_path, **kwds)


def memmap(hdfs_filename, **kwds):
    """
    Create a memory-map to an array stored in a binary file on disk.

     Args:
       :hdfs_filename: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
       :**kwds: You can add any additional args found in numpy.loadtxt(...) 

     Returns:
      A memmap with dtype and shape that matches the data.

     Raises:
      IOError: If the file does not exist
    """
    local_path = _copyHdfsToLocalOverwrite(hdfs_filename)
    return np.fromregex(local_path, **kwds)


def save(hdfs_filename, data):
    """
    Saves a numpy array to a file in HDFS

    Args:
       :hdfs_filename: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS)
       :data: numpy array

    Raises:
      IOError: If the local file does not exist
    """
    local_file = os.path.basename(hdfs_filename)
    np.save(local_file, data)
    _copyToHdfsOverwrite(hdfs_filename)

def savez(hdfs_filename, *args, **kwds):
    """
    Save several arrays into a single file in uncompressed .npz format in HDFS
    If arguments are passed in with no keywords, the corresponding variable names, in the .npz file, are 'arr_0', 'arr_1', etc.
    If keyword arguments are given, the corresponding variable names, in the .npz file will match the keyword names.

    Args:
       :hdfs_filename: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS)
       :args: Arguments, optional
               Arrays to save to the file. Since it is not possible for Python to know the names of the arrays outside savez, 
               the arrays will be saved with names 'arr_0', 'arr_1', and so on. These arguments can be any expression.
       :kwds: Keyword arguments, optional
               Arrays to save to the file. Arrays will be saved in the file with the keyword names.       :data: numpy array

    Returns: None

    Raises:
      IOError: If the local file does not exist
    """
    local_file = os.path.basename(hdfs_filename)
    np.savez(local_file, *args, **kwds)
    _copyToHdfsOverwrite(hdfs_filename)

def savez_compressed(hdfs_filename, *args, **kwds):
    """
    Save several arrays into a single file in uncompressed .npz format in HDFS
    If arguments are passed in with no keywords, the corresponding variable names, in the .npz file, are 'arr_0', 'arr_1', etc.
    If keyword arguments are given, the corresponding variable names, in the .npz file will match the keyword names.

    Args:
       :hdfs_filename: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS)
       :*args: Arguments, optional
               Arrays to save to the file. Since it is not possible for Python to know the names of the arrays outside savez, 
               the arrays will be saved with names 'arr_0', 'arr_1', and so on. These arguments can be any expression.
       :**kwds: Keyword arguments, optional
               Arrays to save to the file. Arrays will be saved in the file with the keyword names.       :data: numpy array

    Returns: None

    Raises:
      IOError: If the local file does not exist
    """
    local_file = os.path.basename(hdfs_filename)
    np.savez_compressed(local_file, *args, **kwds)
    _copyToHdfsOverwrite(hdfs_filename)


def _copyHdfsToLocalOverwrite(hdfs_filename):
    hdfs_path = hdfs._expand_path(hdfs_filename)
    local_path = hdfs.copy_to_local(hdfs_path, overwrite=True)
    return local_path
    
def _copyToHdfsOverwrite(hdfs_filename):
    local_file = os.path.basename(hdfs_filename)
    hdfs_path = hdfs._expand_path(hdfs_filename, exists=False)
    if local_file in hdfs_path:
        # copy_to_hdfs expects directory to copy to, excluding the file name
        hdfs_path = hdfs_path.replace(local_file, "")
    hdfs.copy_to_hdfs(local_file, hdfs_path, overwrite=True)
