"""
API for opening csv files into Pandas from HDFS
"""
import hops.hdfs as hdfs
import pandas as pd

def read_csv(hdfs_filename, **kwds):
    """
      Reads a comma-separated values (csv) file from HDFS into a Pandas DataFrame

      Args:
         :hdfs_filename: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS)
         :**kwds: You can add any additional args found in pandas.read_csv(...) 

      Returns:
        A pandas dataframe

      Raises:
        IOError: If the file does not exist
    """
    hdfs_path = hdfs._expand_path(hdfs_filename)    
    h = hdfs.get_fs()
    with h.open_file(hdfs_path, "rt") as f:
      data = pd.read_csv(f, **kwds)
    return data

def read_parquet(hdfs_filename, **kwds):
    """
      Load a parquet object from a HDFS path, returning a DataFrame.

      Args:
         :hdfs_filename: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS)
         :**kwds: You can add any additional args found in pandas.read_csv(...) 

      Returns:
        A pandas dataframe

      Raises:
        IOError: If the file does not exist
    """
    hdfs_path = hdfs._expand_path(hdfs_filename)    
    h = hdfs.get_fs()
    with h.open_file(hdfs_path, "rt") as f:
      data = pd.read_parquet(f, **kwds)
    return data

def read_json(hdfs_filename, **kwds):
    """
      Convert a JSON string to pandas object.

      Args:
         :hdfs_filename: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS)
         :**kwds: You can add any additional args found in pandas.read_csv(...) 

      Returns:
        A pandas dataframe

      Raises:
        IOError: If the file does not exist
    """
    hdfs_path = hdfs._expand_path(hdfs_filename)    
    h = hdfs.get_fs()
    with h.open_file(hdfs_path, "rt") as f:
      data = pd.read_json(f, **kwds)
    return data

def read_excel(hdfs_filename, **kwds):
    """
      Retrieve pandas object stored in HDFS file, optionally based on where criteria



      Args:
         :hdfs_filename: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS)
         :**kwds: You can add any additional args found in pandas.read_csv(...) 

      Returns:
        A pandas dataframe

      Raises:
        IOError: If the file does not exist
    """
    hdfs_path = hdfs._expand_path(hdfs_filename)    
    h = hdfs.get_fs()
    with h.open_file(hdfs_path, "rt") as f:
      data = pd.read_excel(f, **kwds)
    return data


def write_csv(hdfs_filename, dataframe, **kwds):
    """
      Writes a pandas dataframe to a comma-separated values (csv) text file in HDFS. Overwrites the file if it already exists

      Args:
         :hdfs_filename: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS)
         :dataframe: a Pandas dataframe
         :**kwds: You can add any additional args found in pandas.to_csv(...) 

      Raises:
        IOError: If the file does not exist
    """
    hdfs_path = hdfs._expand_path(hdfs_filename, exists=False)    
    h = hdfs.get_fs()
    with h.open_file(hdfs_path, "wt") as f:
      dataframe.to_csv(f, **kwds)


def write_parquet(hdfs_filename, dataframe, **kwds):
    """
      Writes a pandas dataframe to a parquet file in HDFS. Overwrites the file if it already exists

      Args:
         :hdfs_filename: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS)
         :dataframe: a Pandas dataframe
         :**kwds: You can add any additional args found in pandas.to_parequet(...) 

      Raises:
        IOError: If the file does not exist
    """
    hdfs_path = hdfs._expand_path(hdfs_filename, exists=False)    
    h = hdfs.get_fs()
    with h.open_file(hdfs_path, "wb") as f:
      dataframe.to_parquet(f, **kwds)


def write_json(hdfs_filename, dataframe, **kwds):
    """
      Writes a pandas dataframe to a JSON file in HDFS. Overwrites the file if it already exists

      Args:
         :hdfs_filename: You can specify either a full hdfs pathname or a relative one (relative to your Project's path in HDFS)
         :dataframe: a Pandas dataframe
         :**kwds: You can add any additional args found in pandas.to_json(...) 

      Raises:
        IOError: If the file does not exist
    """
    hdfs_path = hdfs._expand_path(hdfs_filename, exists=False)    
    h = hdfs.get_fs()
    with h.open_file(hdfs_path, "wt") as f:
      dataframe.to_json(f, **kwds)

      
