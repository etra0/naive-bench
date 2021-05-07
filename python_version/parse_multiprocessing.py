import os
import re
import json
import time
import multiprocessing
import pandas as pd

PATH = "../output_old"

def parse(filename, records):
    local_records = list()
    data = None
    try:
        with open(os.path.join(PATH, filename)) as json_file:
            data = json.load(json_file)
    except:
        print(f"Skipping {filename}")
        return False
    for edge in data["data"]["feedback"]["display_comments"]["edges"]:
        local_records.append({
            'id': edge['node']['id'],
            'author_id': edge['node']['author']['id'],
            'author_name': edge['node']['author']['name'],
            'author_gender': edge['node']['author'].get('gender'),
            'timestamp': edge['node']['created_time'],
            'reactions': edge['node']['feedback']['reactors']['count'],
            'url': edge['node']['url'],
            'comment': edge['node']['body']['text'] if edge['node']['body'] else None,
        })
    records.extend(local_records)
    return True

def main():
    manager = multiprocessing.Manager()
    records = manager.list()
    start = time.time()
    with multiprocessing.Pool(4) as p:
        result = p.starmap(parse, [(x, records) for x in  os.listdir(PATH)])

    dataframe = pd.DataFrame.from_records(records)
    dataframe['timestamp'] = pd.to_datetime(dataframe.timestamp, unit='s')
    dataframe.sort_values("id").to_csv(os.path.join("./", 'hora_posting.csv'), index=False)
    elapsed = time.time() - start
    print(f"Speed: {952./elapsed:.2f} MB/s")


if __name__ == "__main__":
    main()
