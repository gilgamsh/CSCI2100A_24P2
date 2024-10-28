#include <iostream>
#include <fstream>
#include <vector>
#include <cmath>
#include <algorithm>
#include <string>

#include "toy_memory.hpp"

int external_merge_sort(int B, int b, int N, int T, std::string input_file, std::string output_file){
    BufferPool buffer_pool(B, b);
    BufferPoolManager buffer_pool_manager(buffer_pool);
    SecStore sec_store;
    SecStoreManager sec_store_manager(sec_store, buffer_pool, buffer_pool_manager, b, T);

    // read the input file from disk to sec store

    // TODO: implement the external merge sort algorithm

    // write the sorted data to the output file

    std::cout << "H: " << sec_store_manager.H<< std::endl;
}


int main() {
    int B = 10000;
    int b = 250;
    int N = 1000000;
    int T = 64;

    // TODO: read from the command line instead of hardcoding
    std::string input_file = "data/input.txt";
    std::string output_file = "data/output.txt";

    external_merge_sort(B, b, N, T, input_file, output_file);

}