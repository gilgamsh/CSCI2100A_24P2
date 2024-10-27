import math
import sys
import heapq
import os


class BufferPool:

    def __init__(self, B, b):
        # TODO: Implement buffer pool
        # self.B = B  # Buffer pool size in words
        # self.b = b  # Block size in words
        # self.pool = [None] * self.B  # Simulate buffer pool with a list
        # self.free_blocks = [True] * (self.B // self.b)  # Block availability
        pass


class BufferPoolManager:
    # TODO: Implement buffer pool manager
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
        # TODO: Implement buffer pool allocation
        pass

    def free(self, start_address, num_blocks):
        """Free space in the buffer pool."""
        # TODO: Implement buffer pool deallocation
        pass


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
        pass

    def write_file(self, file_name):
        """
        The store is also used for the output file sorted.txt that you output
        (in CSV format). 
        """
        pass


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
        # TODO: Implement read operation
        pass

    def write(self, name, start, size, buf_address):
        """Write data from bufPool to secStore."""
        # TODO: Implement write operation
        pass
