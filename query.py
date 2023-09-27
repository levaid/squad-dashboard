from __future__ import annotations

import os.path
import time
from datetime import datetime, timezone
import re

import argparse
import requests
import csv
import schedule
from bs4 import BeautifulSoup


def main():
    args = parse_args()
    job(args.server_id_battlemetrics, args.server_id_squadservers, args.log_folder)
    schedule.every(args.frequency).minutes.do(
        job,
        battlemetrics_id=args.server_id_battlemetrics,
        squadservers_id=args.server_id_squadservers,
        folder=args.log_folder
    )
    while True:
        schedule.run_pending()
        time.sleep(1)


def parse_args():
    parser = argparse.ArgumentParser(description='My Application')
    parser.add_argument('--server_id_battlemetrics', '-B', type=int, help='Battlemetrics ID')
    parser.add_argument('--server_id_squadservers', '-S', type=int, help='Squad-servers.com ID')
    parser.add_argument('--log_folder', type=str, help='Folder for logging', default='./data')
    parser.add_argument('--frequency', type=int, help='Queries every n minutes', default=1)

    args = parser.parse_args()
    return args


def get_server_info(server_id_battlemetrics: int, server_id_squadservers: int) -> str:
    squad_server_info = get_server_info_squad_servers(server_id_squadservers)
    if squad_server_info != 'ERROR':
        return squad_server_info

    battlemetrics_data = get_server_info_battlemetrics(server_id_battlemetrics)
    return battlemetrics_data


def get_server_info_battlemetrics(server_id: int) -> str:
    response = requests.get(f'https://www.battlemetrics.com/servers/squad/{server_id}')
    soup = BeautifulSoup(response.text, features='html.parser')
    server_info = soup.find('div', {'class': 'server-info'})
    if server_info is None:
        return 'ERROR'

    return server_info.get_text(separator=';')


def get_server_info_squad_servers(server_id: int) -> str:
    response = requests.get(f'https://squad-servers.com/server/{server_id}/')
    soup = BeautifulSoup(response.text, features='html.parser')
    server_info = soup.find('table', {'class': 'table table-bordered'})
    if server_info is None:
        return 'ERROR'
    text = server_info.get_text(separator=';')
    return re.sub(r'\s', '', text)


def get_current_time() -> str:
    current_utc_time = datetime.now(timezone.utc)
    formatted_utc_time = current_utc_time.isoformat()
    return formatted_utc_time


def job(battlemetrics_id: int, squadservers_id: int, folder: str):
    with open(os.path.join(folder, 'raw_query_log.csv'), 'a', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        server_info = get_server_info(battlemetrics_id, squadservers_id)
        current_time = get_current_time()
        writer.writerow([current_time, server_info])


if __name__ == '__main__':
    main()
