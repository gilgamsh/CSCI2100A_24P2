# read from inputs/inputs.txt
# output to outputs/sorted.txt
# sort the float numbers in inputs.txt

if __name__ == '__main__':
    with open('inputs/inputs.txt', 'r') as f:
        data = [float(line.strip()) for line in f]
    data.sort()
    with open('outputs/sorted.txt', 'w') as f:
        for value in data:
            f.write(f"{value}\n")