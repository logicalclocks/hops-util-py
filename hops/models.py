"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

from hops import hdfs

import os
import pydoop.hdfs

# model_path could be local or in HDFS, return path in hopsworks where it is placed
def export_model(local_model_path, model_name, model_version):

    project_path = hdfs.project_path()

    # Create directory with model name
    hdfs_handle = hdfs.get()
    model_name_root_directory = project_path + '/Models/' + str(model_name) + '/' + str(model_version) + '/'
    hdfs_handle.create_directory(model_name_root_directory)


    for (path, dirs, files) in os.walk(local_model_path):

        hdfs_export_subpath = path.replace(local_model_path, '')

        current_hdfs_dir = model_name_root_directory + '/' + hdfs_export_subpath

        if not hdfs_handle.exists(current_hdfs_dir):
            hdfs_handle.create_directory(model_name_root_directory)

        for f in files:
            if not hdfs_handle.exists(current_hdfs_dir + '/' + f):
                print("PUT ", path + '/' + f, current_hdfs_dir)
                pydoop.hdfs.put(path + '/' + f, current_hdfs_dir)

        for d in dirs:
            if not hdfs_handle.exists(current_hdfs_dir + '/' + d):
                print("PUT ", path + '/' + d, current_hdfs_dir + '/')
                pydoop.hdfs.put(path + '/' + d, current_hdfs_dir + '/')
        break







