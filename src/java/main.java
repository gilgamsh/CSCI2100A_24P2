import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

class BufferPool {
    // TODO: Implement Buffer Pool
    int B; // Buffer pool size in words
    int b; // Block size in words
    // Simulate Buffer Pool with an array or list
    // Each block availability (free or not)

    public BufferPool(int B, int b) {
        this.B = B;
        this.b = b;
    }
}

class BufferPoolManager {
    // TODO: Implement Buffer Pool Manager
    BufferPool bufferPool;

    public BufferPoolManager(BufferPool bufferPool) {
        this.bufferPool = bufferPool;
    }

    public int allocate(int numBlocks) {
        // Allocate numBlocks in the buffer pool
        // Return the starting block number or -1 if not enough space
        }

    public void free(int startAddress, int numBlocks) {
        // Free numBlocks starting from startAddress
        }
}

class SecStore {
    // TODO: Implement Secondary Storage
    Map<String, List<Double>> symbols = new HashMap<>();

    public void writeFile(String fileName, List<Double> data) throws IOException {
        // Write data from secondary storage to disk
        
    }

    public List<Double> readFile(String fileName) throws IOException {
        // Read data from disk to secondary storage
        
    }
}

class SecStoreManager {
    // TODO: Implement Secondary Storage Manager
    SecStore secStore;
    BufferPool bufferPool;
    BufferPoolManager bufferPoolManager;
    int b;
    int T;
    int H;

    public SecStoreManager(SecStore secStore, BufferPool bufferPool, BufferPoolManager bufferPoolManager, int b, int T) {
        this.secStore = secStore;
        this.bufferPool = bufferPool;
        this.bufferPoolManager = bufferPoolManager;
        this.b = b;
        this.T = T;
    }

    public boolean read(String name, int start, int size, int bufAddress) {
        // Read data from secondary storage to buffer pool
        // TODO: Implement the data transfer logic
        
    }

    public boolean write(String name, int start, int size, int bufAddress) {
        // Write data from buffer pool to secondary storage
        // TODO: Implement the data transfer logic
        
    }
}

public class ExternalMergeSort {

    public static int externalMergeSort(int B, int b, int N, int T, String inputFile, String outputFile) throws IOException {
        BufferPool bufferPool = new BufferPool(B, b);
        BufferPoolManager bufferPoolManager = new BufferPoolManager(bufferPool);
        SecStore secStore = new SecStore();
        SecStoreManager secStoreManager = new SecStoreManager(secStore, bufferPool, bufferPoolManager, b, T);

        // Read the input file from disk to secondary store
        

        // TODO: Implement the external merge sort algorithm

        // Write the sorted data to the output file
        

        System.out.println("H: " + secStoreManager.H);
        return 0;
    }

    public static void main(String[] args) throws IOException {
        int B = 10000;
        int b = 250;
        int N = 1000000;
        int T = 64;

        // TODO: Read from the command line instead of hardcoding
        String inputFile = "data/input.txt";
        String outputFile = "data/output.txt";

        externalMergeSort(B, b, N, T, inputFile, outputFile);
    }
}