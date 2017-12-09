"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

from hops import hdfs

import pydoop.hdfs
import os

# model_path could be local or in HDFS, return path in hopsworks where it is placed
def export_model(model_name, model_path):

    project_path = hdfs.project_path()

    # Create directory with model name
    fs_handle = hdfs.get_fs()
    hdfs_handle = hdfs.get()
    model_name_root_directory = project_path + '/Models/TensorFlow/' + str(model_name)

    if not fs_handle.exists(model_name_root_directory):
        model_version_directory = model_name_root_directory + '/v1'
        hdfs_handle.create_directory(model_version_directory)
        pydoop.hdfs.put(model_path, model_version_directory)
    else:
        # Find current version
        version_directories = fs_handle.list_directory(model_name_root_directory)
        print(version_directories)
        highest_version = 1
        for entry in version_directories:
            try:
                project_path_index = entry['name'].find('/Projects/')
                path, filename = os.path.split(entry['name'][project_path_index:])
                current_version_number = int(filename[1:])
                if current_version_number > highest_version:
                    highest_version = current_version_number
            except:
                print('Found invalid entry in ' + model_name_root_directory)
                print(filename)

        model_version_directory = model_name_root_directory + '/v' + str(highest_version + 1)
        hdfs_handle.create_directory(model_version_directory)
        pydoop.hdfs.put(model_path, model_version_directory)

    return model_name_root_directory