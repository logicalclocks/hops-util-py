"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""
import subprocess

def get_gpu_info():
    # Get the gpu information
    gpu_info = subprocess.check_output(["nvidia-smi", "--format=csv,noheader,nounits",
                                        "--query-gpu=index,memory.total,memory.free,memory.used,utilization.gpu"]).decode()
    gpu_info = gpu_info.split('\n')

    gpu_info_array = []

    # Check each gpu
    for line in gpu_info:
        if len(line) > 0:
            gpu_id, total_memory, free_memory, used_memory, gpu_util = line.split(',')
            gpu_info_array.append((float(total_memory, free_memory, used_memory, gpu_util)))

    return gpu_info_array

def get_num_gpus():
    gpu_info = subprocess.check_output(["nvidia-smi"]).decode()
    gpu_info = gpu_info.split('\n')
    return len(gpu_info)
