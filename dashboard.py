import os.path

import flask
from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd

timeline_df = pd.read_csv(os.path.join('data', 'processed_timeline.csv'))

server = flask.Flask(__name__)
app = Dash(__name__, server=server)

app.layout = html.Div([
    html.H1(children='MAD server statistics', style={'textAlign': 'center'}),
    dcc.Dropdown(timeline_df['layer'].unique(), 'Sumari_Seed_v2', id='dropdown-selection'),
    dcc.Graph(id='graph-content')
])


@callback(
    Output('graph-content', 'figure'),
    Input('dropdown-selection', 'value')
)
def update_graph(value):
    return px.line(timeline_df, x='time', y='player_count')


if __name__ == '__main__':
    app.run(debug=True)
