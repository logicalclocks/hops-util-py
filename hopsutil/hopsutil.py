"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import pydoop.hdfs as hdfs
import os

def get_hdfs():
    return hdfs.open('default', 0, os.getenv("USER"))