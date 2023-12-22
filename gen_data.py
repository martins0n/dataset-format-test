import numpy as np
import pandas as pd


def gen_data(lines=10000, len_=1000):
    data = np.random.randn(lines, len_)
    df = pd.DataFrame({
        'id': range(lines),
        'target': list(data)
    })
    return df

def ram_usage():
    import psutil
    # Process.memory_info is expressed in bytes, so convert to megabytes
    return psutil.Process().memory_info().rss / (1024 * 1024)

def data_size():
    import os
    return os.path.getsize('data.json') / (1024 * 1024)


if __name__ == '__main__':
    ram_usage()

    lines = 1_000_000
    len_ = 1000
    batch_size = 100_000
    
    for i in range(lines // batch_size):
        df = gen_data(batch_size, len_)
        df.to_json(f'data.json', orient='records', lines=True, mode='a')
        print(f'Batch {i} done')
        print(ram_usage())
        
        import os
        print(os.path.getsize('data.json') / (1024 * 1024))
        