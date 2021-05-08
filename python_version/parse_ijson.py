import ijson.backends.yajl2_cffi as ijson
import os
import re
import time

import pandas as pd

start = time.time()
PATH = "../data"
records = list()
for filename in sorted(os.listdir(PATH)):
    data = None
    try:
        with open(os.path.join(PATH, filename)) as json_file:
            data = json_file.read()
        edges = next(ijson.items(data, "data.feedback.display_comments.edges"))
        for edge in edges:
            records.append({
                'id': edge['node']['id'],
                'author_id': edge['node']['author']['id'],
                'author_name': edge['node']['author']['name'],
                'author_gender': edge['node']['author'].get('gender'),
                'timestamp': edge['node']['created_time'],
                'reactions': edge['node']['feedback']['reactors']['count'],
                'url': edge['node']['url'],
                'comment': edge['node']['body']['text'] if edge['node']['body'] else None,
            })
    except Exception as e:
        print(f"Skipping {filename}, {e}")
        break
        continue

dataframe = pd.DataFrame.from_records(records)
dataframe['timestamp'] = pd.to_datetime(dataframe.timestamp, unit='s')
dataframe.sort_values("id").to_csv(os.path.join("./", 'hora_posting.csv'), index=False)
elapsed = time.time() - start
print(f"Speed: {952./elapsed:.2f} MB/s")
