from __future__ import annotations

import datetime
import os.path

from cachetools import cached, TTLCache

import plotly.graph_objects as go
import flask
from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd
import plotly.subplots

TIMELINE_FILE = 'processed_timeline.csv'
MATCH_FILE = 'match_info.csv'

styles = {
    'pre': {
        'border': 'thin lightgrey solid',
        'overflowX': 'scroll'
    }
}

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
        'You can select the interval to inspect by pressing either of the buttons below or '
        'by dragging on the interval selector.')]),
    dcc.Graph(id='overall-timeline'),
    html.Div(children=[
        html.Div(children=[dcc.Graph(
            id='first-row-piechart',
        )], style={'width': '100%', 'display': 'inline-block', 'vertical-align': 'middle'}),
    ], style={'display': 'flex', 'alignItems': 'center', 'justifyContent': 'center'}),

])


@callback(
    Output('first-row-piechart', 'figure'),
    Input('overall-timeline', 'relayoutData')
)
def create_pycharts(relayout):
    df = load_file(MATCH_FILE).query('live == True and player_count >= 30')
    timeframe = get_timeframe(relayout)
    if timeframe is None:
        filtered_df = df
    else:
        starttime, endtime = timeframe
        filtered_df = df.query('time <= @endtime and time >= @starttime')

    grouped_df = filtered_df[['map_name', 'hours']].groupby('map_name').agg(['count', 'sum', 'mean'])
    grouped_df.columns = grouped_df.columns.droplevel()
    grouped_df = grouped_df.reset_index()
    grouped_df['sum'] = grouped_df['sum'].apply(lambda n: round(n, 2))

    gamemode_df = filtered_df[['game_mode', 'hours']].groupby('game_mode').agg(['count', 'sum', 'mean'])
    gamemode_df.columns = gamemode_df.columns.droplevel()
    gamemode_df = gamemode_df.reset_index()

    custom_color_palette = px.colors.qualitative.Dark24
    pie_color_map = dict(zip(filtered_df['map_name'].unique(), custom_color_palette * 2))
    style_data = {
        'color': 'map_name',
        'color_discrete_map': pie_color_map,
        'category_orders': {
            'map_name': sorted(pie_color_map.keys())
        },
    }

    fig = plotly.subplots.make_subplots(
        1,
        3,
        specs=[[{"type": "pie"}, {"type": "pie"}, {"type": "pie"}]],
        horizontal_spacing=0.05,
        vertical_spacing=0
    )

    fig.add_trace(go.Pie(
        name='',
        title='Number of times played',
        labels=grouped_df['map_name'],
        values=grouped_df['count'],
        domain=dict(x=[0, 1 / 3]),
        hovertemplate='%{label}<br>Times played: %{value} (%{percent})',
        marker_colors=[pie_color_map[mapname] for mapname in grouped_df['map_name']],
        direction='clockwise',
        textinfo='label+value',
        textposition='inside',
        insidetextorientation='radial',
        sort=True,
    ), row=1, col=1)
    fig.add_trace(go.Pie(
        name='',
        title='Hours spent on maps total',
        labels=grouped_df['map_name'],
        values=grouped_df['sum'],
        domain=dict(x=[1 / 3, 2 / 3]),
        hovertemplate='%{label}<br>Total hours: %{value:.2f} (%{percent})',
        marker_colors=[pie_color_map[mapname] for mapname in grouped_df['map_name']],
        direction='clockwise',
        textinfo='label+value',
        textposition='inside',
        insidetextorientation='radial',
        sort=True,
    ), row=1, col=2)
    fig.add_trace(go.Pie(
        name='',
        title='Number of times a gamemode is played',
        labels=gamemode_df['game_mode'],
        values=gamemode_df['count'],
        domain=dict(x=[2 / 3, 1]),
        hovertemplate='%{label}<br>Frequency: %{value} (%{percent})',
        marker_colors=[pie_color_map[mapname] for mapname in grouped_df['map_name']],
        direction='clockwise',
        textinfo='label+value',
        textposition='inside',
        insidetextorientation='radial',
        sort=True,
    ), row=1, col=3)
    fig.update_layout(legend=dict(orientation='h'), margin=dict(t=0))
    return fig


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
        color_discrete_sequence=px.colors.qualitative.T10
    )
    fig.update_layout(hovermode='x', dragmode='zoom', selectdirection='h')
    fig.update_xaxes(
        rangeslider_visible=False,
        rangeselector=dict(
            buttons=list([
                dict(count=30, label="30m", step="minute", stepmode="backward"),
                dict(count=1, label="1h", step="hour", stepmode="backward"),
                dict(count=6, label="6h", step="hour", stepmode="backward"),
                dict(count=1, label="1d", step="day", stepmode="backward"),
                dict(count=3, label="3d", step="day", stepmode="backward"),
                dict(count=7, label="1w", step="day", stepmode="backward"),
                dict(count=1, label="1M", step="month", stepmode="backward"),
                dict(step="all")
            ])
        )
    )
    fig.update_yaxes(fixedrange=True)
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
