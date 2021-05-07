import os
import re
import json
import time

import pandas as pd

start = time.time()
PATH = "../output_old"
records = list()
for filename in sorted(os.listdir(PATH)):
    data = None
    try:
        with open(os.path.join(PATH, filename)) as json_file:
            data = json.load(json_file)
    except:
        print(f"Skipping {filename}")
        continue
    for edge in data["data"]["feedback"]["display_comments"]["edges"]:
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

dataframe = pd.DataFrame.from_records(records)
dataframe['timestamp'] = pd.to_datetime(dataframe.timestamp, unit='s')
dataframe.sort_values("id").to_csv(os.path.join("./", 'hora_posting.csv'), index=False)
elapsed = time.time() - start
print(f"Speed: {952./elapsed:.2f} MB/s")
