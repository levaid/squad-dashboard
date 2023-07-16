import os.path

from cachetools import cached, TTLCache

import flask
from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd

TIMELINE_FILE = 'processed_timeline.csv'
MATCH_FILE = 'match_info.csv'


server = flask.Flask(__name__)
app = Dash(__name__, server=server)  # type: ignore


@cached(cache=TTLCache(maxsize=5, ttl=60))
def load_file(filename: str) -> pd.DataFrame:
    return pd.read_csv(os.path.join('data', filename))


app.layout = html.Div([
    html.H1(children='MAD server statistics', style={'textAlign': 'center'}),
    dcc.Dropdown(['Sumari_Seed_v2'], 'Sumari_Seed_v2', id='dropdown-selection'),
    dcc.Graph(id='graph-content')
])


@callback(
    Output('graph-content', 'figure'),
    Input('dropdown-selection', 'value')
)
def update_graph(value):
    df = load_file(TIMELINE_FILE)
    fig = px.line(df, x='time', y='player_count', hover_data=['layer'], color='seeding')
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


if __name__ == '__main__':
    app.run(debug=True)
