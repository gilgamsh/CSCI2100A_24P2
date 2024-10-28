import math
import sys
import heapq
import os
from toy_memory import BufferPool, BufferPoolManager, SecStore, SecStoreManager

import random

def external_merge_sort(B, b, N, T, input_file, output_file):
    """Main function to perform external merge sort."""

    #############################
    # Initialization
    #############################

    # Initialize buffer pool and managers
    buffer_pool = BufferPool(B, b)
    buffer_pool_manager = BufferPoolManager(buffer_pool)
    sec_store = SecStore()
    sec_man = SecStoreManager(sec_store, buffer_pool, buffer_pool_manager, b, T)

    # Read input file and store in secStore
    sec_store.read_file(input_file)
    
    # TODO: Implement external merge sort

    # Write sorted data to output file
    sorted_data = sec_store.symbols['output']
    with open(output_file, 'w') as f:
        for value in sorted_data:
            f.write(f"{value}\n")

    # Print overhead and statistics
    print(f"Total overhead H: {sec_man.H}")
    

# Example usage:
if __name__ == "__main__":
    # Parameters
    B = 10000  # Buffer pool size in words
    b = 250    # Block size in words
    N = 200000  # Number of records
    T = 64     # Relative time taken for secStore access

    # TODO: read from the command line instead of hardcoding
    input_file = "inputs/inputs.txt"
    output_file = "outputs/sorted.txt"

    external_merge_sort(B, b, N, T, input_file, output_file)