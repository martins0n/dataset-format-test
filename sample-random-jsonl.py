from gen_data import ram_usage, data_size

def create_index_file(jsonl_file_path, index_file_path):
    offsets = []
    with open(jsonl_file_path, 'rb') as file:
        offset = 0
        for line in file:
            offsets.append(offset)
            offset += len(line)

    with open(index_file_path, 'w') as file:
        for offset in offsets:
            file.write(f"{offset}\n")

import numpy as np
import random
import time


class RandomBatchReader:
    def __init__(self, jsonl_file_path, index_file_path):
        self.jsonl_file_path = jsonl_file_path
        self.index_file_path = index_file_path
        

        with open(index_file_path, 'r') as file:
            self.offsets = [int(line.strip()) for line in file]

        self.file = open(jsonl_file_path, 'rb')
        
        self.offsets = np.array(self.offsets)

    def __del__(self):
        self.file.close()

    def read_random_batch(self, batch_size):
        random_offsets = np.random.choice(self.offsets, batch_size)
        batch_lines = []

        for offset in random_offsets:
            self.file.seek(offset)
            line = self.file.readline().decode('utf-8').strip()
            batch_lines.append(line)

        return batch_lines
    
    def read_batch(self, batch_size):
        for i in range(len(self.offsets) // batch_size):
            batch_lines = []
            for offset in self.offsets[i * batch_size : (i + 1) * batch_size]:
                self.file.seek(offset)
                line = self.file.readline().decode('utf-8').strip()
                batch_lines.append(line)
            yield batch_lines





if __name__ == '__main__':
    jsonl_file_path = 'data.json'
    index_file_path = 'data_index_file.txt'
    batch_size = 100
    sample_rate = 20
    
    print(f'RAM usage before: {ram_usage()}')
    
    create_index_file(jsonl_file_path, index_file_path)
    
    list_times = []
    
    reader = RandomBatchReader(jsonl_file_path, index_file_path)
    
    
    for i in range(sample_rate):
        start_time = time.time()
        random_batch = reader.read_random_batch(batch_size)
        end_time = time.time()
        print(f'Batch {i} done')
        print(f'RAM usage: {ram_usage()}')
        
        list_times.append(end_time - start_time)
        
    print(f'Average time: {sum(list_times) / len(list_times)}')
    print(f'Min time: {min(list_times)}')
    print(f'Max time: {max(list_times)}')
    
    
    start_time = time.time()
    
    print('>>>')
    print('>>>')

    for batch in reader.read_batch(batch_size):
        pass
    print(f'RAM usage: {ram_usage()}')
    
    end_time = time.time()
    
    print(f'Time: {end_time - start_time}')
    
    # GB per second
    print(f'GB per second: {data_size() / 1024 / (end_time - start_time)}')
