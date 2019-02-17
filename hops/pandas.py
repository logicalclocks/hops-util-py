"""
API for opening csv files into Pandas from HopsFS.
"""

def read_csv(hdfs_filename, **kwds):
  import hops.hdfs as hdfs
  import pandas as pd
  h = hdfs.get_fs()
  with h.open_file(hdfs_filename, "rt") as f:
    data = pd.read_csv(f, **kwds)
  return data

