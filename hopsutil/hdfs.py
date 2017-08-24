"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import pydoop.hdfs as pyhdfs
import os

def get():
    return pyhdfs.hdfs('default', 0, os.getenv("USER"))
