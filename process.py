import argparse
import logging
import os
import re
import time
from functools import lru_cache

import numpy as np
import pandas as pd
import schedule

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def parse_args():
    parser = argparse.ArgumentParser(description='My Application')
    parser.add_argument('--log_folder', type=str, help='Folder for logging', default='./data')

    args = parser.parse_args()
    return args


def process(log_folder: str):
    starttime = time.time()
    raw_data_with_errors = pd.read_csv(os.path.join(log_folder, 'raw_query_log.csv'), names=['time', 'data'])
    timeline_data = create_timeline(raw_data_with_errors)
    timeline_data.to_csv(os.path.join(log_folder, 'processed_timeline.csv'), index=False)
    match_data = create_match_data(timeline_data)
    match_data.to_csv(os.path.join(log_folder, 'match_info.csv'), index=False)
    event_data = create_event_log(timeline_data)
    event_data.to_csv((os.path.join(log_folder, 'seed_live_info.csv')), index=False)
    endtime = time.time()
    logging.info(f'Job took {endtime - starttime:.2f} seconds.')
    return True


def create_event_log(timeline_df: pd.DataFrame) -> pd.DataFrame:
    df = timeline_df.copy()
    player_count_log = df['player_count'].values
    layer_log = df['layer'].values
    # events = {'seed', 'live', 'dying', 'dead'}
    event_log = [(0, 'dead')]
    event_happening = False
    current_event = 'dead'
    for i, (player_count, layer) in enumerate(zip(player_count_log, layer_log)):
        is_seeding_layer = 'seed' in layer.lower()
        if current_event == 'dead':
            if player_count < 5:
                continue
            elif player_count >= 5:
                current_event = 'seed'
                event_happening = True
        elif current_event == 'seed':
            if player_count <= 2:
                current_event = 'dead'
                event_happening = True
            elif 2 < player_count <= 50:
                continue
            elif 50 < player_count <= 60 and not is_seeding_layer:  # mapchanged from seeding to live layer
                current_event = 'live'
                event_happening = True
            elif player_count > 60:
                current_event = 'live'
                event_happening = True
        elif current_event == 'live':
            if player_count < 5:
                current_event = 'dead'
                event_happening = True
            elif player_count < 50:
                current_event = 'dying'
                event_happening = True
            elif player_count >= 50:
                pass
        elif current_event == 'dying':
            if player_count < 2:
                current_event = 'dead'
                event_happening = True
            elif 2 <= player_count < 60:
                pass
            elif player_count >= 60:
                current_event = 'live'
                event_happening = True
        if event_happening:
            event_log.append((i, current_event))
            event_happening = False

    if current_event in {'live', 'seed'}:  # we finished on Live, we have to add this manually:
        event_log.append((len(player_count_log) - 1, current_event))

    data = [{
        'event': event,
        'time': df.iloc[i]['time'],
    } for i, event in event_log]
    event_df = pd.DataFrame.from_records(data)
    event_df['date'] = event_df['time'].apply(lambda d: d.date())

    event_df['previous_event'] = event_df['event'].shift(1)
    event_df['previous_event_time'] = event_df['time'].shift(1)
    event_df['duration'] = event_df.apply(lambda row: (row['time'] - row['previous_event_time']).total_seconds(),
                                          axis=1)
    event_df['hours'] = event_df['duration'].apply(lambda t: round(t / 3600, 2))
    return event_df


def parse_battlemetrics_data(d: str):
    layer = get_regex_from_data(d, re.compile(r'Map;(\w+);'), '')
    return {
        'player_count': int(get_regex_from_data(d, re.compile(r'Player count;(\d+)'), '-1')),
        'layer': layer,
        'seeding': 'seed' in layer.lower(),
        'source': 'Battlemetrics'
    }


def parse_squadservers_data(d: str):
    layer = get_regex_from_data(d, re.compile(r'Map;;(\w+)'), '')
    return {
        'player_count': int(get_regex_from_data(d, re.compile(r'Players;;(\d+)/'), '-1')),
        'layer': layer,
        'seeding': 'seed' in layer.lower(),
        'source': 'Squad-servers.com'
    }


def process_row(d: str) -> dict:
    if d == 'ERROR':
        return {}
    if d.startswith('Rank'):
        return parse_battlemetrics_data(d)
    if d.startswith(';;;'):
        return parse_squadservers_data(d)
    print(f'error with {d}')
    return {}


def create_timeline(raw_data_with_errors: pd.DataFrame) -> pd.DataFrame:
    raw_data = raw_data_with_errors.query('data != "ERROR"').copy()
    df = raw_data[['time']].copy().reset_index()
    df['time'] = pd.to_datetime(df['time'])
    processed_data = raw_data['data'].apply(process_row)
    timeline_df = pd.json_normalize(processed_data)
    df = pd.concat([df[['time']], timeline_df], axis=1).query('layer != "Unknown"')
    df['player_change_15_mins'] = df[['player_count', 'time']] \
        .rolling('15min', on='time') \
        .apply((lambda x: x.iloc[-1] - x.iloc[0]))['player_count']
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
    mapchange_indices = zip([0] + list(layer_df.index)[:-1],
                            list(layer_df.index))  # dirty trick to get the index intervals
    mapchange_player_counts = [np.mean(df['player_count'].loc[start: stop]) for start, stop in mapchange_indices]

    # Calculate the time difference for each row compared to the previous row
    layer_df['time_diff'] = layer_df['time'].diff()
    layer_df['mean_player_count'] = mapchange_player_counts
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
    if layer_name.count('_') != 2:
        return 'unknown', 'N/A', 'N/A'
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
