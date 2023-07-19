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
    seed_data = create_seed_info(timeline_data)
    event_data = create_event_log(timeline_data)
    event_data.to_csv((os.path.join(log_folder, 'seed_live_info.csv')), index=False)
    return True


def create_event_log(timeline_df: pd.DataFrame) -> pd.DataFrame:
    df = timeline_df.copy()
    player_count_log = df['player_count'].values
    # events = {'seed', 'live', 'dying', 'dead'}
    event_log = [(0, 'dead')]
    event_happening = False
    current_event = 'dead'
    for i, player_count in enumerate(player_count_log):
        if current_event == 'dead':
            if player_count <= 5:
                continue
            if player_count > 5:
                current_event = 'seed'
                event_happening = True
        elif current_event == 'seed':
            if player_count <= 5:
                current_event = 'dead'
                event_happening = True
            if 5 < player_count <= 60:
                continue
            if player_count > 60:
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
            if player_count < 5:
                current_event = 'dead'
                event_happening = True
            if 5 <= player_count < 60:
                pass
            if player_count >= 60:
                current_event = 'live'
                event_happening = True
        if event_happening:
            event_log.append((i, current_event))
            event_happening = False

    if current_event in {'live', 'seed'}:  # we finished on Live, we have to add this manually:
        event_log.append((len(player_count_log)-1, current_event))

    data = [{
        'event': event,
        'time': df.loc[i]['time'],
    } for i, event in event_log]
    event_df = pd.DataFrame.from_records(data)
    event_df['date'] = event_df['time'].apply(lambda d: d.date())

    event_df['previous_event'] = event_df['event'].shift(1)
    event_df['previous_event_time'] = event_df['time'].shift(1)
    event_df['duration'] = event_df.apply(lambda row: (row['time'] - row['previous_event_time']).total_seconds(), axis=1)
    event_df['hours'] = event_df['duration'].apply(lambda t: round(t / 3600, 2))
    return event_df


def custom_below(s: pd.Series) -> int:
    if s.sum() == 0:
        return s.index[-1]
    return s.idxmax()


def create_seed_info(timeline_df: pd.DataFrame) -> pd.DataFrame:
    # Initialize a variable to keep track of the start of the current interval
    df = timeline_df.copy()
    df['player_count'] = df['player_count'].astype(int)
    df['time'] = pd.to_datetime(df['time'])
    start_index = 0

    # Lists to store the intervals
    intervals_seeding = []
    intervals_live = []

    while start_index < len(df):
        # Select the part of the DataFrame that hasn't been considered yet
        current_df = df.iloc[start_index:]

        # Find the first indices of the conditions
        first_exceeds_5 = (current_df['player_count'] >= 5).idxmax()
        first_exceeds_60 = (current_df['player_count'] >= 60).idxmax()
        first_drops_below_30 = custom_below(df.iloc[first_exceeds_60:]['player_count'] <= 30)
        first_drops_below_5 = custom_below(df.iloc[first_exceeds_5:]['player_count'] == 0)

        if first_exceeds_5 == first_exceeds_60:  # we are seeding!
            intervals_seeding.append((first_exceeds_5, first_drops_below_5))  # this should point to the end
            break

        if first_exceeds_60 != start_index:
            intervals_seeding.append((first_exceeds_5, first_exceeds_60))
            intervals_live.append((first_exceeds_60, first_drops_below_30))

        # Update the start_index for the next iteration to the index after the one where player_count drops to 0
        # If player_count never drops to 0, this will be the index past the end of the DataFrame, and the loop will end
        start_index = first_drops_below_5 + 1

    data = [{
        'event': 'seed',
        'duration': (df['time'][end] - df['time'][start]).total_seconds(),
        'starttime': df['time'][start],
        'endtime': df['time'][end]
    } for start, end in intervals_seeding]

    data += [{
        'event': 'live',
        'duration': (df['time'][end] - df['time'][start]).total_seconds(),
        'starttime': df['time'][start],
        'endtime': df['time'][end]
    } for start, end in intervals_live]

    seed_df = pd.DataFrame.from_records(data).sort_values('starttime')
    return seed_df


def create_timeline(raw_data_with_errors: pd.DataFrame) -> pd.DataFrame:
    raw_data = raw_data_with_errors.query('data != "ERROR"').copy()
    df = raw_data[['time']].copy()
    df['time'] = pd.to_datetime(df['time'])
    df['player_count'] = raw_data['data'].apply(
        lambda d: get_regex_from_data(d, re.compile(r'Player count;(\d+);'), '-1')
    ).apply(int)
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
