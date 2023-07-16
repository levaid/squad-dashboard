from __future__ import annotations

import datetime
import json
import os.path

from cachetools import cached, TTLCache

import flask
from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd

TIMELINE_FILE = 'processed_timeline.csv'
MATCH_FILE = 'match_info.csv'

styles = {
    'pre': {
        'border': 'thin lightgrey solid',
        'overflowX': 'scroll'
    }
}

COLOR_PALETTES = {'Sage and brown': ['#7b876d', '#989e8b', '#ddb8a6', '#d49b7e', '#c67f43', '#893f04'],
                  'Warm earth': ['#f7d3c7', '#daa095', '#a5888a', '#927071', '#c89566', '#756156'],
                  'Earth tone': ['#996663', '#cda39f', '#e0b39c', '#e8ce95', '#c1d0c6', '#a1aba3'][::-1],
                  'Craigslist': ['#fbb260', '#f57d62', '#e15b64', '#abbd83', '#849b87'],
                  'Paper flower': ['#edbdae', '#cf786f', '#eaaa65', '#becccf', '#6e7c76', '#826b5e'],
                  'Summer 01': ['#fdc98a', '#f9b185', '#e89697', '#f4cdc8', '#c7c09b', '#969f8e'],
                  'Cheerful neighborhood': ['#ffefd7', '#ffceae', '#f9ab89', '#e68472', '#f3c9cc', '97667b']}

server = flask.Flask(__name__)
app = Dash(__name__, server=server, title='MAD server statistics')  # type: ignore


@cached(cache=TTLCache(maxsize=5, ttl=60))
def load_file(filename: str) -> pd.DataFrame:
    return pd.read_csv(os.path.join('data', filename))


app.layout = html.Div([
    dcc.Interval(
        id="load-interval",
        n_intervals=0,
        max_intervals=0,  # <-- only run once
        interval=1
    ),
    html.H1(children='MAD server statistics', style={'textAlign': 'center'}),
    html.Div(id='instruction', children=[html.P(
        'You can select the interval to inspect by pressing either of the button below or'
        'by dragging on the interval selector.')]),
    dcc.Graph(id='overall-timeline'),
    html.Div(children=[
        html.Div(children=[dcc.Graph(
            id='piechart-1',
        )], style={'width': '30%', 'display': 'inline-block', 'vertical-align': 'middle'}),
        html.Div(children=[dcc.Graph(
            id='piechart-2',
        )], style={'width': '30%', 'display': 'inline-block', 'vertical-align': 'middle'}),
    ], style={'display': 'flex', 'alignItems': 'center', 'justifyContent': 'center'}),

])


@callback(
    Output('piechart-1', 'figure'),
    Output('piechart-2', 'figure'),
    Input('overall-timeline', 'relayoutData')
)
def create_pycharts(relayout):
    df = load_file(MATCH_FILE).query('live == True')
    timeframe = get_timeframe(relayout)
    if timeframe is None:
        filtered_df = df
    else:
        starttime, endtime = timeframe
        filtered_df = df.query('time <= @endtime and time >= @starttime')

    custom_color_palette = COLOR_PALETTES['Sage and brown']
    pie_color_map = dict(zip(filtered_df['map_name'].unique(), custom_color_palette * 2))
    style_data = {
        'color': 'map_name',
        'color_discrete_map': pie_color_map,
        'category_orders': {
            'map_name': sorted(pie_color_map.keys())
        },
    }
    piechart_number_of_times = px.pie(
        filtered_df,
        names='map_name',
        title='Number of times the map was played',
        **style_data
    )
    piechart_time_spent = px.pie(
        filtered_df,
        values='hours',
        names='map_name',
        title='Time spent on map',
        **style_data
    )
    return piechart_number_of_times, piechart_time_spent


@callback(
    Output('overall-timeline', 'figure'),
    Input('load-interval', 'n_intervals'),
)
def update_timeline(_n_intervals: int):
    df = load_file(TIMELINE_FILE)
    fig = px.line(
        df,
        x='time',
        y='player_count',
        hover_data=['layer'],
        color='seeding',
        color_discrete_sequence=px.colors.qualitative.T10
    )
    fig.update_layout(hovermode='x', dragmode='select', selectdirection='h')
    fig.update_xaxes(
        rangeslider_visible=True,
        rangeselector=dict(
            buttons=list([
                dict(count=30, label="30m", step="minute", stepmode="todate"),
                dict(count=1, label="1h", step="hour", stepmode="todate"),
                dict(count=6, label="6h", step="hour", stepmode="todate"),
                dict(count=1, label="1d", step="day", stepmode="todate"),
                dict(count=7, label="1w", step="day", stepmode="todate"),
                dict(count=1, label="1M", step="month", stepmode="todate"),
                dict(step="all")
            ])
        )
    )
    return fig


def get_timeframe(data: dict | None) -> tuple[datetime.time, datetime.time] | None:
    if not data:
        return None
    if 'xaxis.range' in data:
        return data['xaxis.range'][0], data['xaxis.range'][1]
    if 'xaxis.range[0]' in data:
        return data['xaxis.range[0]'], data['xaxis.range[1]']
    return None


if __name__ == '__main__':
    app.run(debug=True)
