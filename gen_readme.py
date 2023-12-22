import json

with open('README.md', 'w') as file:
    with open('sample_stats.json', 'r') as file1:
        lines = file1.readlines()

    file.write('# Sample stats\n\n')
    file.write('| Test case | Min time sample | Max time sample| Avg time sample | GB per second | RAM usage | Data size |\n')
    file.write('| --------- | -------- | -------- | -------- | ------------- | --------- | --------- |\n')
    for line in lines:
        stats = json.loads(line)
        min_time = round(stats["min"], 2)
        max_time = round(stats["max"], 2)
        avg_time = round(stats["avg"], 2)
        iter_speed = round(stats["iter_speed"], 2)
        ram_usage = round(stats["ram_usage"], 2)
        data_size = round(stats["data_size"], 2)
        file.write(f'| {stats["test_case"].split("/")[-1]} | {min_time} | {max_time} | {avg_time} | {iter_speed} | {ram_usage} | {data_size} |\n')
