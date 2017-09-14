"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import pydoop.hdfs as hdfs
import os

def get():
    return hdfs.hdfs('default', 0, user=project_user())

def project_path():
    hops_user = os.environ["SPARK_USER"]
    hops_user_split = hops_user.split("__")
    project = hops_user_split[0]
    return hdfs.path.abspath("/Projects/" + project + "/")

def project_user():
    hops_user = os.environ["SPARK_USER"]
    return hops_user