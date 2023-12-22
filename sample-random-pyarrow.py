import pyarrow as pa
import pyarrow.json as paj
import numpy as np
import random
import time
from gen_data import ram_usage

# Function to load data into a PyArrow table
def load_data_to_arrow_table(jsonl_file_path):
    # return paj.read_json(jsonl_file_path)

class RandomBatchReader:
    def __init__(self, arrow_table):
        self.arrow_table = arrow_table
        self.num_rows = arrow_table.num_rows

    def read_random_batch(self, batch_size):
        # Select random indices
        random_indices = np.random.randint(0, self.num_rows, batch_size)
        # Retrieve rows corresponding to these indices
        return self.arrow_table.take(random_indices)

if __name__ == '__main__':
    jsonl_file_path = 'data.json'
    batch_size = 100
    sample_rate = 20
    
    print(f'RAM usage before: {ram_usage()}')
    
    # Load data into an Arrow table
    arrow_table = load_data_to_arrow_table(jsonl_file_path)

    reader = RandomBatchReader(arrow_table)
    
    list_times = []
    
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

