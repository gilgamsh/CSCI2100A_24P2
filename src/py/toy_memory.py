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
            return -1  # Not enough space

    def free(self, start_address, num_blocks):
        """Free space in the buffer pool."""
        start_block = start_address // self.buffer_pool.b
        for i in range(start_block, start_block + num_blocks):
            self.buffer_pool.free_blocks[i] = True
        self.stats['deallocations'] += 1
        self.stats['freed_blocks'] += num_blocks

class SecStore:
    def __init__(self):
        self.files = {}  # Simulate secondary storage files

class SecStoreManager:
    def __init__(self, sec_store, buffer_pool, buffer_pool_manager, b, T):
        self.sec_store = sec_store
        self.buffer_pool = buffer_pool
        self.buffer_pool_manager = buffer_pool_manager
        self.b = b
        self.T = T
        self.H = 0  # Total overhead

    def read(self, file_name, file_offset, size, buf_address):
        """Read data from secStore to bufPool."""
        if file_name not in self.sec_store.files:
            print(f"File {file_name} does not exist in secStore.")
            return False
        data = self.sec_store.files[file_name][file_offset:file_offset + size]
        if len(data) != size:
            print(f"Could not read the requested size from {file_name}.")
            return False
        # Copy data to buffer pool
        self.buffer_pool.pool[buf_address:buf_address + size] = data
        # Update overhead
        blocks_accessed = math.ceil(size / self.b)
        self.H += blocks_accessed * self.T
        return True

    def write(self, file_name, file_offset, size, buf_address):
        """Write data from bufPool to secStore."""
        # Get data from buffer pool
        data = self.buffer_pool.pool[buf_address:buf_address + size]
        # Write data to secStore
        if file_name not in self.sec_store.files:
            self.sec_store.files[file_name] = []
        # Ensure the file list is large enough
        file_data = self.sec_store.files[file_name]
        if len(file_data) < file_offset:
            file_data.extend([None] * (file_offset - len(file_data)))
        if len(file_data) < file_offset + size:
            file_data.extend([None] * (file_offset + size - len(file_data)))
        file_data[file_offset:file_offset + size] = data
        self.sec_store.files[file_name] = file_data
        # Update overhead
        blocks_accessed = math.ceil(size / self.b)
        self.H += blocks_accessed * self.T
        return True