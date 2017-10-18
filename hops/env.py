"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""
import subprocess

def get_gpu_info():
    # Get the gpu information
    gpu_str = ''
    try:
        gpu_info = subprocess.check_output(["nvidia-smi", "--format=csv,noheader,nounits", "--query-gpu=name,memory.total,memory.used,utilization.gpu"]).decode()
        gpu_info = gpu_info.split('\n')
    except:
        gpu_str = 'Could not find any GPUs accessible for the container'
        return gpu_str

    # Check each gpu
    gpu_str = ''
    for line in gpu_info:
        if len(line) > 0:
            name, total_memory, memory_used, gpu_util = line.split(',')
            gpu_str += '\nType: ' + name + '\n'
            gpu_str += 'Total memory: ' + total_memory + '\n'
            gpu_str += 'Currently allocated memory: ' + memory_used + '\n'
            gpu_str += 'Current utilization: ' + gpu_util + '\n'
            gpu_str += '\n'

    return gpu_str



def get_num_gpus():
    gpu_info = subprocess.check_output(["nvidia-smi", "--format=csv,noheader,nounits", "--query-gpu=name"]).decode()
    gpu_info = gpu_info.split('\n')

    count = 0
    for line in gpu_info:
        if len(line) > 0:
            count += 1
    return count
