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
       :**kwds: You can add any additional args found in numpy.read(...) 

     Returns:
      A numpy array

     Raises:
      IOError: If the file does not exist
    """
    hdfs_path = hdfs._expand_path(hdfs_filename)
    local_path = hdfs.copy_to_local(hdfs_path)
    return np.load(local_path, **kwds)

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
    hdfs_path = hdfs._expand_path(hdfs_filename, exists=False)
    if local_file in hdfs_path:
        # copy_to_hdfs expects directory to copy to, excluding the file name
        hdfs_path = hdfs_path.replace(local_file, "")
    hdfs.copy_to_hdfs(local_file, hdfs_path, overwrite=True)

