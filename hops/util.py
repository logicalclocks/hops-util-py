"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import os
import sys

def _find_in_path(path, file):
    """Find a file in a given path string."""
    for p in path.split(os.pathsep):
        candidate = os.path.join(p, file)
        if (os.path.exists(os.path.join(p, file))):
            return candidate
    return False

def find_tensorboard():
    pypath = sys.executable
    pydir = os.path.dirname(pypath)
    search_path = os.pathsep.join([pydir, os.environ['PATH'], os.environ['PYTHONPATH']])
    tb_path = _find_in_path(search_path, 'tensorboard')
    if not tb_path:
        raise Exception("Unable to find 'tensorboard' in: {}".format(search_path))
    return tb_path