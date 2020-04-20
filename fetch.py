#!/usr/bin/env python3

import pathlib
import subprocess

PREFIX="gs://predictionmachine/data/finnhub/candle5"

def gs_cp(src, dst):
    cmd = ['gsutil', 'cp', src, dst]
    print(" ".join(cmd))
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f'expected 0 return code on cp {src} {dst}, but got {result.returncode}')


def fetch(grp, date):
    target = pathlib.Path('.') / 'data' / 'finnhub' / grp / f'{date}_all_stocks.timesorted.csv.gz'
    target.parent.mkdir(parents = True, exist_ok = True)

    gs_cp(
        f'{PREFIX}/{grp}/{date}/all_stocks.timesorted.csv.gz',
        str(target))

if __name__ == "__main__":
    fetch("HK", "2020-02-15_2020-04-14")
    fetch("L", "2020-02-16_2020-04-15")
    fetch("T", "2020-02-16_2020-04-15")
