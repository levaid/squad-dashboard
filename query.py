from __future__ import annotations

import time
from datetime import datetime, timezone

import argparse
import requests
import csv
import schedule
from bs4 import BeautifulSoup


def parse_args():
    parser = argparse.ArgumentParser(description='My Application')
    parser.add_argument('--server_id', type=int, help='An ID for the server')

    args = parser.parse_args()
    return args


def get_server_info(server_id: int) -> str:
    response = requests.get(f'https://www.battlemetrics.com/servers/squad/{server_id}')
    soup = BeautifulSoup(response.text, features='html.parser')
    server_info = soup.find('div', {'class': 'server-info'})
    if server_info is None:
        'ERROR'

    return server_info.get_text(separator=';')


def get_current_time() -> str:
    current_utc_time = datetime.now(timezone.utc)
    formatted_utc_time = current_utc_time.isoformat()
    return formatted_utc_time


def job(server_id: int, filename: str):
    with open(filename, 'a', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        server_info = get_server_info(server_id)
        current_time = get_current_time()
        writer.writerow([current_time, server_info])


def main():
    args = parse_args()
    schedule.every(5).minutes.do(job, server_id=args.server_id, filename='query_log.csv')
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    main()
