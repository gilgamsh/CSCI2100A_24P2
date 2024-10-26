import math
import sys
import heapq
import os
import toy_memory

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
    with open(input_file, 'r') as f:
        data = [float(line.strip()) for line in f]
    sec_store.files['input'] = data

    # Determine the number of initial runs
    run_size = B  # Number of words that can be sorted in memory
    num_runs = math.ceil(N / run_size)

    #############################
    # First Pass: Create Sorted Runs
    #############################
    for run in range(num_runs):
        # Calculate the number of blocks needed
        size = min(run_size, N - run * run_size)
        num_blocks_needed = math.ceil(size / b)
        # Allocate buffer space
        start_address = buffer_pool_manager.allocate(num_blocks_needed)
        if start_address == -1:
            print("Not enough buffer pool space for initial sorting.")
            sys.exit(1)

        # Read data from secStore into buffer pool
        file_offset = run * run_size
        if not sec_man.read('input', file_offset, size, start_address):
            print("Failed to read data from secStore.")
            sys.exit(1)

        # Sort data in buffer pool
        data_segment = buffer_pool.pool[start_address:start_address + size]
        data_segment.sort()
        buffer_pool.pool[start_address:start_address + size] = data_segment

        # Write sorted run back to secStore
        run_file_name = f"run_{run}"
        if not sec_man.write(run_file_name, 0, size, start_address):
            print("Failed to write sorted run to secStore.")
            sys.exit(1)

        # Free buffer space
        buffer_pool_manager.free(start_address, num_blocks_needed)

    #############################
    # Second Pass: Merge Runs
    #############################

    # Determine buffer distribution
    k = num_runs  # Number of runs to merge
    # Decide buffer sizes for input buffers and output buffer
    input_buffer_size = B // (k + 1)
    output_buffer_size = B - k * input_buffer_size
    if input_buffer_size == 0 or output_buffer_size == 0:
        print("Buffer pool size is too small to perform merge.")
        sys.exit(1)

    # Initialize buffers and positions
    input_buffers = {}
    input_positions = {}
    input_totals = {}
    run_file_offsets = {}
    for run in range(num_runs):
        input_buffers[run] = []
        input_positions[run] = 0
        input_totals[run] = 0
        run_file_offsets[run] = 0

    output_buffer = []
    output_total = 0

    # Open output file in secStore
    sec_store.files['output'] = []

    # Main merge loop
    while True:
        # Refill input buffers if needed
        for run in range(num_runs):
            if input_positions[run] >= input_totals[run]:
                # Need to read new data into input buffer
                size = min(input_buffer_size, N - run_file_offsets[run])
                if size > 0:
                    # Calculate the number of blocks needed
                    num_blocks_needed = math.ceil(size / b)
                    # Allocate buffer space
                    start_address = buffer_pool_manager.allocate(num_blocks_needed)
                    if start_address == -1:
                        print("Not enough buffer pool space for input buffers.")
                        sys.exit(1)
                    # Read data from secStore
                    run_file_name = f"run_{run}"
                    if not sec_man.read(run_file_name, run_file_offsets[run], size, start_address):
                        print(f"Failed to read data from run {run}.")
                        sys.exit(1)
                    # Store data in input buffer
                    data = buffer_pool.pool[start_address:start_address + size]
                    # Free buffer space
                    buffer_pool_manager.free(start_address, num_blocks_needed)
                    input_buffers[run] = data
                    input_positions[run] = 0
                    input_totals[run] = len(data)
                    run_file_offsets[run] += size
                else:
                    input_buffers[run] = []
                    input_positions[run] = 0
                    input_totals[run] = 0

        # Check if all buffers are empty
        if all(input_totals[run] == 0 for run in range(num_runs)):
            # All data has been merged
            break

        # Build a min-heap of the first elements of each input buffer
        heap = []
        for run in range(num_runs):
            if input_positions[run] < input_totals[run]:
                heapq.heappush(heap, (input_buffers[run][input_positions[run]], run))
                input_positions[run] += 1

        while heap:
            value, run = heapq.heappop(heap)
            # Append value to output buffer
            output_buffer.append(value)
            output_total += 1
            if len(output_buffer) >= output_buffer_size:
                # Write output buffer to secStore
                size = len(output_buffer)
                num_blocks_needed = math.ceil(size / b)
                # Allocate buffer space
                start_address = buffer_pool_manager.allocate(num_blocks_needed)
                if start_address == -1:
                    print("Not enough buffer pool space for output buffer.")
                    sys.exit(1)
                buffer_pool.pool[start_address:start_address + size] = output_buffer
                # Write to secStore
                if not sec_man.write('output', len(sec_store.files['output']), size, start_address):
                    print("Failed to write output buffer to secStore.")
                    sys.exit(1)
                # Free buffer space
                buffer_pool_manager.free(start_address, num_blocks_needed)
                # Clear output buffer
                output_buffer = []
            # Get next element from the same run
            if input_positions[run] < input_totals[run]:
                heapq.heappush(heap, (input_buffers[run][input_positions[run]], run))
                input_positions[run] += 1
            elif run_file_offsets[run] < N:
                # Refill input buffer for this run
                size = min(input_buffer_size, N - run_file_offsets[run])
                if size > 0:
                    num_blocks_needed = math.ceil(size / b)
                    # Allocate buffer space
                    start_address = buffer_pool_manager.allocate(num_blocks_needed)
                    if start_address == -1:
                        print("Not enough buffer pool space for input buffers.")
                        sys.exit(1)
                    # Read data from secStore
                    run_file_name = f"run_{run}"
                    if not sec_man.read(run_file_name, run_file_offsets[run], size, start_address):
                        print(f"Failed to read data from run {run}.")
                        sys.exit(1)
                    # Store data in input buffer
                    data = buffer_pool.pool[start_address:start_address + size]
                    # Free buffer space
                    buffer_pool_manager.free(start_address, num_blocks_needed)
                    input_buffers[run] = data
                    input_positions[run] = 0
                    input_totals[run] = len(data)
                    run_file_offsets[run] += size
                    # Push next element to heap
                    heapq.heappush(heap, (input_buffers[run][input_positions[run]], run))
                    input_positions[run] += 1

    # Write any remaining output buffer to secStore
    if len(output_buffer) > 0:
        size = len(output_buffer)
        num_blocks_needed = math.ceil(size / b)
        # Allocate buffer space
        start_address = buffer_pool_manager.allocate(num_blocks_needed)
        if start_address == -1:
            print("Not enough buffer pool space for output buffer.")
            sys.exit(1)
        buffer_pool.pool[start_address:start_address + size] = output_buffer
        # Write to secStore
        if not sec_man.write('output', len(sec_store.files['output']), size, start_address):
            print("Failed to write output buffer to secStore.")
            sys.exit(1)
        # Free buffer space
        buffer_pool_manager.free(start_address, num_blocks_needed)

    #############################
    # Output Results
    #############################

    # Write sorted data to output file
    sorted_data = sec_store.files['output']
    with open(output_file, 'w') as f:
        for value in sorted_data:
            f.write(f"{value}\n")

    # Print overhead and statistics
    print(f"Total overhead H: {sec_man.H}")
    print("Buffer Pool Manager Statistics:")
    print(buffer_pool_manager.stats)

# Example usage:
if __name__ == "__main__":
    # Parameters
    B = 10000  # Buffer pool size in words
    b = 250    # Block size in words
    N = 200000  # Number of records
    T = 64     # Relative time taken for secStore access

    input_file = "inputs.txt"
    output_file = "sorted.txt"

    # Generate test data if input file does not exist
    if not os.path.exists(input_file):
        with open(input_file, 'w') as f:
            for _ in range(N):
                f.write(f"{random.uniform(0, 1000)}\n")

    external_merge_sort(B, b, N, T, input_file, output_file)