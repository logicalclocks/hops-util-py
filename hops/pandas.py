"""
API for opening csv files into Pandas from HopsFS.
"""

def read_csv(hdfs_filename, **kwds):
    """
      Reads a comma-separated values (csv) file from HopsFS into a Pandas DataFrame.

      Args:
         :hdfs_filename: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS).
         :**kwds: You can add any additional args found in pandas.read_csv(...) 

      Returns:
        A pandas dataframe

      Raises:
        IOError: If the file does not exist.
    """
  import hops.hdfs as hdfs
  import pandas as pd
  hdfs_path = hdfs._expand_path(hdfs_filename)    
  h = hdfs.get_fs()
  with h.open_file(hdfs_path, "rt") as f:
    data = pd.read_csv(f, **kwds)
  return data

