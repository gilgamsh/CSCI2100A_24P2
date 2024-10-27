import math
import sys
import heapq
import os


class BufferPool:

    def __init__(self, B, b):
        self.B = B  # Buffer pool size in words
        self.b = b  # Block size in words
        self.pool = [None] * self.B  # Simulate buffer pool with a list
        self.free_blocks = [True] * (self.B // self.b)  # Block availability


class BufferPoolManager:

    def __init__(self, buffer_pool):
        self.buffer_pool = buffer_pool
        # WARN: useless
        self.stats = {
            'allocations': 0,
            'deallocations': 0,
            'allocated_blocks': 0,
            'freed_blocks': 0
        }

    def allocate(self, num_blocks):
        """Allocate space in the buffer pool."""
        consecutive_free = 0
        start_block = -1
        for i in range(len(self.buffer_pool.free_blocks)):
            if self.buffer_pool.free_blocks[i]:
                if consecutive_free == 0:
                    start_block = i
                consecutive_free += 1
                if consecutive_free == num_blocks:
                    break
            else:
                consecutive_free = 0
                start_block = -1
        if consecutive_free == num_blocks:
            for i in range(start_block, start_block + num_blocks):
                self.buffer_pool.free_blocks[i] = False
            self.stats['allocations'] += 1
            self.stats['allocated_blocks'] += num_blocks
            start_address = start_block * self.buffer_pool.b
            return start_address
        else:
            return -1  

    def free(self, start_address, num_blocks):
        """Free space in the buffer pool."""
        start_block = start_address // self.buffer_pool.b
        for i in range(start_block, start_block + num_blocks):
            self.buffer_pool.free_blocks[i] = True
        self.stats['deallocations'] += 1
        self.stats['freed_blocks'] += num_blocks


class SecStore:
    """
    read files from disk and write files to disk
    """

    def __init__(self):
        self.symbols = {}


    def read_file(self, file_name):
        """
        SecStore is used for the input file inputs.txt, a text file containing
        floating point numbers to be sorted. 
        """
        with open(file_name, 'r') as file:
            data = [float(line.strip()) for line in file]
            self.symbols['input'] = data

    def write_file(self, file_name):
        """
        The store is also used for the output file sorted.txt that you output
        (in CSV format). 
        """
        with open(file_name, 'w') as file:
            for line in self.symbols[file_name]:
                file.write(line)


class SecStoreManager:

    def __init__(self, sec_store, buffer_pool, buffer_pool_manager, b, T):
        self.sec_store = sec_store
        self.buffer_pool = buffer_pool
        self.buffer_pool_manager = buffer_pool_manager
        self.b = b
        self.T = T
        self.H = 0  # Total overhead

    def read(self, name, start, size, buf_address):
        """Read data from secStore to bufPool."""
        if name not in self.sec_store.symbols:
            print(f"file/array {name} does not exist in secStore.")
            return False
        data = self.sec_store.symbols[name][start:start + size]
        if len(data) != size:
            print(f"Not enough data in file/array {name}. Expected {size}, got {len(data)}.")
            
        size = len(data)
        # Copy data to buffer pool
        self.buffer_pool.pool[buf_address:buf_address + size] = data
        # Update overhead
        blocks_accessed = math.ceil(size / self.b)
        self.H += blocks_accessed * self.T
        return True

    def write(self, name, start, size, buf_address):
        """Write data from bufPool to secStore."""
        # Get data from buffer pool
        buffer_data = self.buffer_pool.pool[buf_address:buf_address + size]
        if len(buffer_data) != size:
            print(f"Not enough data in buffer pool. Expected {size}, got {len(buffer_data)}.")
        size = len(buffer_data)
        # Write data to secStore
        if name not in self.sec_store.symbols:
            self.sec_store.symbols[name] = []
        # Ensure the file list is large enough
        data = self.sec_store.symbols[name]
        if len(data) < start:
            data.extend([None] * (start - len(data)))
        if len(data) < start + size:
            data.extend([None] * (start + size - len(data)))
        data[start:start + size] = buffer_data
        self.sec_store.symbols[name] = data
        # Update overhead
        blocks_accessed = math.ceil(size / self.b)
        self.H += blocks_accessed * self.T
        return True
