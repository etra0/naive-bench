import os
import re
import argparse
import subprocess

import pandas as pd
import matplotlib.pyplot as plt

parse_speed = re.compile(r"Speed: (.*) MB/s")

class Program:
    __original_path: str
    __basedir: str
    __command: str
    def __init__(self, basedir, command):
        self.__original_path = os.getcwd()
        self.__basedir = basedir
        self.__command = command

    def run(self): 
        os.chdir(self.__basedir)
        output = subprocess.run(self.__command, check=True, capture_output=True)
        print(output.stdout.decode(encoding='utf-8'))
        match = parse_speed.search(str(output.stdout))
        result = match.group(1)
        os.chdir(self.__original_path)
        return float(result)

def generate_data():
    to_run = {
        "python_ijson": Program("python_version", ['bash', '-c', "cd $(PWD) && source ./venv/bin/activate && python3 parse_ijson.py"]),
        "python": Program("python_version", "python3 parse.py".split(" ")),
        "python_mp": Program("python_version", "python3 parse_multiprocessing.py".split(" ")),
        "rust_serde": Program("rust_version", "cargo r --release -- ../data/".split(" ")),
        "rust_simd": Program("rust_version", "cargo r --release --features simd_json -- ../data/".split(" ")),
        "go": Program("go_version", "go run main.go ../data/".split(" ")),
    }

    records = []
    for name, program in to_run.items():
        print(f"Executing {name}")
        for _ in range(5):
            records.append({
                'name': name,
                "speed": program.run()
            })
    df = pd.DataFrame.from_records(records).to_csv("timing.csv", index=False)

def plot():
    df = pd.read_csv("timing.csv")
    with plt.xkcd():
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.set_title("impl vs speed (MB/s), best of 5")
        cmap = plt.get_cmap('viridis')
        (df
         .groupby("name")
         .speed.max()
         .sort_values()
         .plot.barh(ax=ax, cmap=cmap))
        fig.tight_layout()
        fig.savefig("results.png", dpi=300)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate & Process data")
    parser.add_argument('action', metavar='A', type=str,
            help="generate, plot")
    args = parser.parse_args()
    if args.action == 'generate':
        generate_data()
    elif args.action == 'plot':
        plot()
    else:
        print('Incorrect argument')
