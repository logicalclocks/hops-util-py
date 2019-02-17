"""
API for reading/writing numpy arrays to/from HopsFS.
"""

def loadnp(hdfs_filename, **kwds):
    """
    Reads a file from HopsFS into a Numpy Array

     Args:
       :hdfs_filename: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
       :**kwds: You can add any additional args found in numpy.read(...) 

     Returns:
      A numpy array

     Raises:
      IOError: If the file does not exist.
    """
    import hops.hdfs as hdfs
    import numpy as np
    hdfs_path = hdfs._expand_path(hdfs_filename)
    local_path = hdfs.localize(hdfs_path)
    return np.load(local_path, **kwds)

def savenp(hdfs_filename, data):
    """
    Saves a numpy array to a file in HopsFS 

    Args:
       :hdfs_filename: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
       :data: numpy array

    Raises:
      IOError: If the local file does not exist.
    """
    import hops.hdfs as hdfs
    hdfs_path = hdfs._expand_path(hdfs_filename, exists=False)
    with hdfs.get_fs().open_file(hdfs_path, 'w') as data_file:
      data_file.write(data)

