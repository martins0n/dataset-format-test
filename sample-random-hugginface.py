from gen_data import ram_usage, data_size
from datasets import load_dataset
import numpy as np
import random
import time
import json

def load_jsonl_dataset(jsonl_file_path):
    """Loads a jsonl file as a Hugging Face dataset."""
    return load_dataset('json', data_files=jsonl_file_path, split='train')

class RandomBatchReader:
    """A reader class that uses Hugging Face datasets for reading random batches."""
    def __init__(self, dataset):
        self.dataset = dataset

    def read_random_batch(self, batch_size):
        """Reads a random batch of entries from the dataset."""
        indices = np.random.randint(0, len(self.dataset), batch_size)
        return [self.dataset[int(i)] for i in indices]

    def read_batch(self, batch_size):
        """Reads sequential batches of entries from the dataset."""
        return self.dataset.iter(batch_size=batch_size)

def measure_performance(reader, batch_size, sample_rate):
    """Measures and prints the performance of the RandomBatchReader."""
    list_times = []
    for i in range(sample_rate):
        start_time = time.time()
        reader.read_random_batch(batch_size)
        end_time = time.time()
        list_times.append(end_time - start_time)
        print(f'Batch {i} done. RAM usage: {ram_usage()}')
    
    print_stats(list_times)
    return list_times

def print_stats(list_times):
    """Prints statistical information about the times."""
    print(f'Average time: {np.mean(list_times)}')
    print(f'Min time: {np.min(list_times)}')
    print(f'Max time: {np.max(list_times)}')

if __name__ == '__main__':
    jsonl_file_path = 'data.json'
    batch_size = 100
    sample_rate = 20

    print(f'RAM usage before: {ram_usage()}')
    dataset = load_jsonl_dataset(jsonl_file_path)

    reader = RandomBatchReader(dataset)
    list_times = measure_performance(reader, batch_size, sample_rate)

    sample_stats = {
        'max': max(list_times),
        'min': min(list_times),
        'avg': sum(list_times) / len(list_times),
        'test_case': __file__.split('.')[0],
        'iter_time': None,
        'ram_usage': None,
        'data_size': None,
        'iter_speed': None
    }

    # Sequential reading
    start_time = time.time()
    for _ in reader.read_batch(batch_size):
        pass

    end_time = time.time()

    sample_stats['iter_time'] = end_time - start_time
    sample_stats['ram_usage'] = ram_usage()
    sample_stats['data_size'] = data_size() / 1024
    sample_stats['iter_speed'] = data_size() / 1024 / (end_time - start_time)

    print(f'GB per second: {sample_stats["iter_speed"]}')

    with open('sample_stats.json', 'a') as file:
        file.write(json.dumps(sample_stats))
        file.write('\n')
