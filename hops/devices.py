"""

Utility functions to retrieve information about available devices in the environment.

"""
import subprocess
import time
import threading
import os

def _get_gpu_info():
    """
    Get the gpu information

    Returns:

    """

    if count_nvidia_gpus() > 0:
        gpu_info = subprocess.check_output(["nvidia-smi", "--format=csv,noheader,nounits", "--query-gpu=name,memory.total,memory.used,utilization.gpu"]).decode()
        gpu_info = gpu_info.split('\n')
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
    elif count_rocm_gpus() > 0 and not 'HIP_VISIBLE_DEVICES' in os.environ:
        return subprocess.check_output(["/opt/rocm/bin/rocm-smi", "--showallinfo"]).decode()
    else:
        return '\nCould not find any GPUs accessible for the container\n'

def _get_nvidia_gpu_util():
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

def _get_rocm_gpu_util():
    """

    Returns:

    """
    gpu_str = ''
    try:
        gpu_info = subprocess.check_output(["/opt/rocm/bin/rocm-smi", "--showuse"]).decode()
    except:
        return gpu_str

    return gpu_info

def _print_periodic_gpu_utilization():
    """

    Returns:

    """
    t = threading.currentThread()
    rocm_gpu = count_rocm_gpus()
    nvidia_gpu = count_nvidia_gpus()
    while getattr(t, "do_run", True):
        time.sleep(10)
        if rocm_gpu > 0 and not 'HIP_VISIBLE_DEVICES' in os.environ:
            print(_get_rocm_gpu_util())
        if nvidia_gpu > 0:
            print(_get_nvidia_gpu_util())

def count_nvidia_gpus():
    try:
        p = subprocess.Popen(['which nvidia-smi'], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (output,err)=p.communicate()
        returncode = p.wait()
        if not returncode == 0:
            return 0
        process = subprocess.Popen("nvidia-smi -L", shell=True, stdout=subprocess.PIPE)
        stdout_list = process.communicate()[0].split('\n')
        return len(stdout_list)-1
    except Exception as err:
        return 0

def count_rocm_gpus():
    try:
        p = subprocess.Popen(['which /opt/rocm/bin/rocm-smi'], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (output,err)=p.communicate()
        returncode = p.wait()
        if not returncode == 0:
            return 0
        process = subprocess.Popen("/opt/rocm/bin/rocm-smi -i | grep GPU", shell=True, stdout=subprocess.PIPE)
        stdout_list = process.communicate()[0].split('\n')
        return len(stdout_list)-1
    except Exception as err:
        return 0

def get_num_gpus():
    try:
        return max(count_nvidia_gpus(), count_rocm_gpus())
    except Exception as err:
        return 0