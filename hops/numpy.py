"""
API for reading/writing numpy arrays to/from HopsFS.
"""

def loadnp(hdfs_filename):
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
    return np.load(hdfs.localize(hdfs_path))

def savenp(data, hdfs_filename):
    """
    Saves a numpy array to a file in HopsFS 

    Args:
       :data: numpy array
       :hdfs_filename: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).

    Raises:
      IOError: If the file does not exist.
    """
    import hops.hdfs as hdfs
    hdfs_path = hdfs._expand_path(hdfs_filename)
    with hdfs.get_fs().open_file(hdfs_path, 'w') as data_file:
      data_file.write(data)

