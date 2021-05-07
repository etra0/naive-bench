import os
import re
import subprocess

import pandas as pd
import matplotlib.pyplot as plt

parse_speed = re.compile(r"Speed: (.*) MB/s")

class Program:
    __original_path: str
    __dest_dir: str
    __command: list
    def __init__(self, dest_dir, command):
        self.__original_path = os.getcwd()
        self.__dest_dir = dest_dir
        self.__command = command.split(" ")

    def run(self): 
        os.chdir(self.__dest_dir)
        output = subprocess.run(self.__command, check=True, capture_output=True)
        match = parse_speed.search(str(output.stdout))
        result = match.group(1)
        os.chdir(self.__original_path)
        return float(result)

def generate_data():
    to_run = {
        "rust_serde": Program("rust_version", "cargo r --release -- ../output_old/"),
        "rust_simd": Program("rust_version", "cargo r --release --features simd_json -- ../output_old/"),
        "go": Program("go_version", "go run main.go ../output_old/"),
        "python": Program("python_version", "python3 parse.py"),
        "python_mp": Program("python_version", "python3 parse_multiprocessing.py"),
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
        cmap = plt.get_cmap('inferno')
        (df
         .groupby("name")
         .speed.max()
         .sort_values()
         .plot.barh(ax=ax, cmap=cmap))
        fig.tight_layout()
        fig.savefig("out.png", dpi=300)

if __name__ == "__main__":
    # generate_data()
    plot()
