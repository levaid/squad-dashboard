import argparse
import os
import time

import schedule

import pandas as pd
import re


def parse_args():
    parser = argparse.ArgumentParser(description='My Application')
    parser.add_argument('--log_folder', type=str, help='Folder for logging', default='./data')

    args = parser.parse_args()
    return args


def process(log_folder: str):
    raw_data = pd.read_csv(os.path.join(log_folder, 'raw_query_log.csv'), names=['time', 'data'])
    df = raw_data[['time']].copy()
    df['player_count'] = raw_data['data'].apply(
        lambda d: get_regex_from_data(d, re.compile(r'Player count;(\d+);'), '-1')
    )
    df['layer'] = raw_data['data'].apply(
        lambda d: get_regex_from_data(d, re.compile(r'Map;(\w+);'), '')
    )
    df.to_csv(os.path.join(log_folder, 'processed_timeline.csv'), index=False)
    return df


def get_regex_from_data(data: str, pattern: re.Pattern, default: str) -> str:
    match = re.search(pattern, data)
    if match:
        return match.group(1)
    return default


def main():
    args = parse_args()
    process(args.log_folder)
    schedule.every(1).minutes.do(process, log_folder=args.log_folder)
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    main()
