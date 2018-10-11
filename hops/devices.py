"""

Utility functions to retrieve information about available devices in the environment.

"""
import subprocess
import time
import threading

def _get_gpu_info():
    """
    Get the gpu information

    Returns:

    """
    gpu_str = ''
    try:
        gpu_info = subprocess.check_output(["nvidia-smi", "--format=csv,noheader,nounits", "--query-gpu=name,memory.total,memory.used,utilization.gpu"]).decode()
        gpu_info = gpu_info.split('\n')
    except:
        gpu_str = '\nCould not find any GPUs accessible for the container\n'
        return gpu_str

    # Check each gpu
    gpu_str = ''
    for line in gpu_info:
        if len(line) > 0:
            name, total_memory, memory_used, gpu_util = line.split(',')
            gpu_str += '\nName: ' + name + '\n'
            gpu_str += 'Total memory: ' + total_memory + '\n'
            gpu_str += 'Currently allocated memory: ' + memory_used + '\n'
            gpu_str += 'Current utilization: ' + gpu_util + '\n'
            gpu_str += '\n'

    return gpu_str

def _get_gpu_util():
    """

    Returns:

    """
    gpu_str = ''
    try:
        gpu_info = subprocess.check_output(["nvidia-smi", "--format=csv,noheader,nounits", "--query-gpu=name,memory.total,memory.used,utilization.gpu"]).decode()
        gpu_info = gpu_info.split('\n')
    except:
        return gpu_str

    gpu_str = '\n------------------------------ GPU usage information ------------------------------\n'
    for line in gpu_info:
        if len(line) > 0:
            name, total_memory, memory_used, gpu_util = line.split(',')
            gpu_str += '[Type: ' + name + ', Memory Usage: ' + memory_used + ' /' + total_memory + ' (MB), Current utilization: ' + gpu_util + '%]\n'
    gpu_str += '-----------------------------------------------------------------------------------\n'
    return gpu_str

def _print_periodic_gpu_utilization():
    """

    Returns:

    """
    t = threading.currentThread()
    while getattr(t, "do_run", True):
        print(_get_gpu_util())
        time.sleep(10)

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
        gpu_info = subprocess.check_output(["nvidia-smi", "--format=csv,noheader,nounits", "--query-gpu=name"]).decode()
        gpu_info = gpu_info.split('\n')
    except:
        return 0

    count = 0
    for line in gpu_info:
        if len(line) > 0:
            count += 1
    return count

def _get_minor_gpu_device_numbers():
    """

    Returns:

    """
    gpu_info = []
    try:
        gpu_info = subprocess.check_output(["nvidia-smi", "--format=csv,noheader,nounits", "--query-gpu=pci.bus_id"]).decode()
    except:
        return gpu_info

    gpu_info = gpu_info.split('\n')
    device_id_list = []
    for line in gpu_info:
        if len(line) > 0:
            pci_bus_id = line.split(',')
            device_id_list.append(pci_bus_id)





