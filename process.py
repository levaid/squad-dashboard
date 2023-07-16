import argparse
import os
import re
import time
from functools import lru_cache

import pandas as pd
import schedule


def parse_args():
    parser = argparse.ArgumentParser(description='My Application')
    parser.add_argument('--log_folder', type=str, help='Folder for logging', default='./data')

    args = parser.parse_args()
    return args


def process(log_folder: str):
    raw_data_with_errors = pd.read_csv(os.path.join(log_folder, 'raw_query_log.csv'), names=['time', 'data'])
    timeline_data = create_timeline(raw_data_with_errors)
    timeline_data.to_csv(os.path.join(log_folder, 'processed_timeline.csv'), index=False)
    match_data = create_match_data(timeline_data)
    match_data.to_csv(os.path.join(log_folder, 'match_info.csv'), index=False)
    return True


def create_timeline(raw_data_with_errors: pd.DataFrame) -> pd.DataFrame:
    raw_data = raw_data_with_errors.query('data != "ERROR"').copy()
    df = raw_data[['time']].copy()
    df['player_count'] = raw_data['data'].apply(
        lambda d: get_regex_from_data(d, re.compile(r'Player count;(\d+);'), '-1')
    )
    df['layer'] = raw_data['data'].apply(
        lambda d: get_regex_from_data(d, re.compile(r'Map;(\w+);'), '')
    )
    df['seeding'] = df['layer'].apply(lambda s: 'seed' in s.lower())

    return df


def create_match_data(timeline_df: pd.DataFrame) -> pd.DataFrame:
    df = timeline_df.copy()
    # I am assuming that your dataframe is named df
    # Make sure your 'time' column is in datetime format
    df['time'] = pd.to_datetime(df['time'])
    df['previous_layer'] = df['layer'].shift(1)
    # Create a new column that indicates when 'layer' changes
    df['layer_changed'] = df['layer'].ne(df['previous_layer'])
    layer_df = df.query('layer_changed == True').dropna(subset='previous_layer').copy()

    # Calculate the time difference for each row compared to the previous row
    layer_df['time_diff'] = layer_df['time'].diff()
    layer_df['hours'] = layer_df['time_diff'].apply(lambda t: t.total_seconds() / 3600)

    layer_df['minutes'] = layer_df['time_diff'].apply(lambda t: t.total_seconds() / 60)
    layer_df['map_name'] = layer_df['previous_layer'].apply(lambda l: split_layer(l)[0])
    layer_df['game_mode'] = layer_df['previous_layer'].apply(lambda l: split_layer(l)[1])
    layer_df['version'] = layer_df['previous_layer'].apply(lambda l: split_layer(l)[2])
    layer_df['live'] = layer_df['game_mode'] != 'Seed'
    # Forward fill the NA values
    # df['time_diff_since_last_change'] = df['time_diff_since_last_change'].fillna(method='ffill')

    return layer_df.dropna(subset='previous_layer')


@lru_cache(512)
def split_layer(layer_name: str) -> tuple[str, str, str]:
    mapname, gamemode, version, *_ = layer_name.split('_')
    return mapname, gamemode, version


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
