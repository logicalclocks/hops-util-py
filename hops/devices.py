"""

Utility functions to retrieve information about available devices in the environment.

"""
import subprocess
import time
import threading
import os
from pynvml import *
import fnmatch

import logging
log = logging.getLogger(__name__)

def _get_gpu_info():
    """
    Get the gpu information

    Returns:

    """

    if _count_nvidia_gpus() > 0:
        try:
            gpu_str = '\n------------------------------ Found GPU device ------------------------------\n'
            nvmlInit()
            device_count = int(nvmlDeviceGetCount())
            for i in range(device_count):
                handle = nvmlDeviceGetHandleByIndex(i)
                name = nvmlDeviceGetName(handle).decode("utf-8")
                mem_info = nvmlDeviceGetMemoryInfo(handle)
                util = nvmlDeviceGetUtilizationRates(handle)
                gpu_str += '[Type: ' + name + ', Memory Usage: ' + str(int(mem_info.used/1000000)) + '/' + str(int(mem_info.total/1000000)) + ' (MB), Current utilization: ' + str(int(util.gpu)) + '%]\n'
            gpu_str += '-----------------------------------------------------------------------------------\n'
            return gpu_str
        except Exception as err:
            log.error(err)
            pass
        finally:
            try:
                nvmlShutdown()
            except:
                pass
    else:
        return 'Could not find any GPUs accessible for the container'

def _get_nvidia_gpu_util():
    """

    Returns:

    """
    try:
        gpu_str = '\n------------------------------ GPU usage information ------------------------------\n'
        nvmlInit()
        device_count = int(nvmlDeviceGetCount())
        for i in range(device_count):
            handle = nvmlDeviceGetHandleByIndex(i)
            name = nvmlDeviceGetName(handle).decode("utf-8")
            mem_info = nvmlDeviceGetMemoryInfo(handle)
            util = nvmlDeviceGetUtilizationRates(handle)
            gpu_str += '[Type: ' + name + ', Memory Usage: ' + str(int(mem_info.used/1000000)) + '/' + str(int(mem_info.total/1000000)) + ' (MB), Current utilization: ' + str(int(util.gpu)) + '%]\n'
        gpu_str += '-----------------------------------------------------------------------------------\n'
        return gpu_str
    except Exception as err:
        log.error(err)
        pass
    finally:
        try:
            nvmlShutdown()
        except Exception as err:
            log.error(err)
            pass

    return 'No GPU utilization information available, failed to initialize NVML'

def _print_periodic_gpu_utilization():
    """

    Returns:

    """
    t = threading.currentThread()
    nvidia_gpu = _count_nvidia_gpus()
    while getattr(t, "do_run", True):
        time.sleep(10)
        if nvidia_gpu > 0:
            print(_get_nvidia_gpu_util())

def _count_nvidia_gpus():
    try:
        if 'EXECUTOR_GPUS' in os.environ:
            return int(os.environ['EXECUTOR_GPUS'])
        nvmlInit()
        return int(nvmlDeviceGetCount())
    except Exception as err:
        return 0
    finally:
        try:
            nvmlShutdown()
        except:
            pass

def get_num_gpus():
    """ Get the number of GPUs available in the environment and consequently by the application
    Assuming there is one GPU in the environment

    >>> from hops import devices
    >>> devices.get_num_gpus()
    >>> 1

    Returns:
        Number of GPUs available in the environment
    """
    try:
        return _count_nvidia_gpus()
    except Exception as err:
        return 0