"""

Utility functions to retrieve information about available devices in the environment.

"""
import subprocess
import time
import threading
import os
from pynvml import *
import fnmatch

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
            print(err)
            pass
        finally:
            try:
                nvmlShutdown()
            except:
                pass
    elif _count_rocm_gpus() > 0 and not 'HIP_VISIBLE_DEVICES' in os.environ:
        return subprocess.check_output(["/opt/rocm/bin/rocm-smi", "--showallinfo"], shell=True).decode("utf-8")
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
        print(err)
        pass
    finally:
        try:
            nvmlShutdown()
        except Exception as err:
            print(err)
            pass

    return 'No GPU utilization information available, failed to initialize NVML'

def _get_rocm_gpu_util():
    """

    Returns:

    """
    gpu_str = ''
    try:
        gpu_info = subprocess.check_output(["/opt/rocm/bin/rocm-smi", "--showuse"], shell=True).decode("utf-8")
    except Exception as err:
        print(err)
        return gpu_str

    return gpu_info

def _print_periodic_gpu_utilization():
    """

    Returns:

    """
    t = threading.currentThread()
    nvidia_gpu = _count_nvidia_gpus()
    rocm_gpu = 0
    # rocm-smi does not ignore GPUs and will show all of them so rely on environment variable
    if nvidia_gpu == 0:
        rocm_gpu = _count_rocm_gpus()
    while getattr(t, "do_run", True):
        time.sleep(10)
        if nvidia_gpu > 0:
            print(_get_nvidia_gpu_util())
        elif rocm_gpu > 0:
            print(_get_rocm_gpu_util())

def _count_nvidia_gpus():
    try:
        nvmlInit()
        device_count = int(nvmlDeviceGetCount())
        return device_count
    except Exception as err:
        return 0
    finally:
        try:
            nvmlShutdown()
        except:
            pass

def _count_rocm_gpus():
    try:
        if 'EXECUTOR_GPUS' in os.environ: # Exported 0 on driver and num_gpus on executor
            return int(os.environ['EXECUTOR_GPUS'])
        elif os.path.exists('/opt/rocm/bin/rocm-smi') and os.path.exists('/sys/module/amdgpu/drivers/pci:amdgpu'):
            num_gpus = 0
            root = '/sys/module/amdgpu/drivers/pci:amdgpu'
            contents =  os.listdir(root)
            gpu_pci_folders = fnmatch.filter(contents, '*:*:*.*')
            for gpu_pci_folder in gpu_pci_folders:
                if os.path.exists(root + '/' + gpu_pci_folder + '/drm'):
                    drm = os.listdir(root + '/' + gpu_pci_folder + '/drm')
                    cards = fnmatch.filter(drm, 'card*')
                    num_gpus = num_gpus + len(cards)
            return num_gpus
        else:
            return 0
    except Exception as err:
        print(err)
    return 0

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
        num_nvidia_gpus = _count_nvidia_gpus()
        num_rocm_gpus = 0
        # rocm-smi does not ignore GPUs and will show all of them so rely on environment variable
        if num_nvidia_gpus == 0:
            num_rocm_gpus = _count_rocm_gpus()
        return max(num_rocm_gpus, num_nvidia_gpus)
    except Exception as err:
        return 0