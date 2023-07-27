from __future__ import annotations

import datetime
import os.path
from collections import defaultdict

from cachetools import cached, TTLCache

import plotly.graph_objects as go
import flask
from dash import Dash, html, dcc, callback, Output, Input, dash_table
import plotly.express as px
import pandas as pd
import plotly.subplots

TIMELINE_FILE = 'processed_timeline.csv'
MATCH_FILE = 'match_info.csv'
SEED_LIVE_FILE = 'seed_live_info.csv'

styles = {
    'pre': {
        'border': 'thin lightgrey solid',
        'overflowX': 'scroll'
    }
}

pretty_events = {
    'seed': 'seeding',
    'live': 'live',
    'dead': 'dead',
    'dying': 'dying'
}

server = flask.Flask(__name__)
app = Dash(__name__, server=server, title='MAD server statistics',
           url_base_pathname='/squad-dashboard/')  # type: ignore


@cached(cache=TTLCache(maxsize=5, ttl=60))
def load_file(filename: str) -> pd.DataFrame:
    return pd.read_csv(os.path.join('data', filename))


@cached(cache=TTLCache(maxsize=5, ttl=60))
def get_map_color_palette(_cache_key: str) -> dict[str, str]:
    df = pd.read_csv(os.path.join('data', 'match_info.csv'))
    custom_color_palette = px.colors.qualitative.Dark24
    colormap = dict(zip(df['map_name'].unique(), custom_color_palette * 2))
    return colormap


app.layout = html.Div([
    dcc.Interval(
        id="load-interval",
        n_intervals=0,
        max_intervals=0,  # <-- only run once
        interval=1
    ),
    html.H1(children=[html.Span('MAD server statistics by Cucuska2 '),
                      html.Span('('),
                      html.Img(src=app.get_asset_url('goblin.png'), style={'height': '24px'}),
                      html.Span(')'),
                      ],
            style={'textAlign': 'center'}),
    html.Div(id='instruction', children=[html.P(
        'You can select the interval to inspect by pressing either of the buttons below or '
        'by dragging on the interval selector.', style={'padding-left': '5%', 'padding-right': '5%'})]),
    html.Div(id='server-status', style={'padding-left': '5%', 'padding-right': '5%'}),
    dcc.Graph(id='overall-timeline'),
    html.Div(children=[
        html.Div(children=[dcc.Graph(
            id='first-row-piechart',
        )], style={'width': '100%', 'display': 'inline-block', 'vertical-align': 'middle'}),
    ], style={'display': 'flex', 'alignItems': 'center', 'justifyContent': 'center'}),
    html.Div([
        html.Div(id='match-table', style={'width': '50%'}),
        dcc.Graph(id='frequent-layers', style={'width': '50%'})
    ], style={
        'display': 'flex',
        'alignItems': 'center',
        'justifyContent': 'center',
        'padding-left': '5%',
        'padding-right': '5%'
    }),
    html.Div(children=[
        dcc.Graph(id='seed-timeline')
    ])
])


def filter_df_for_timeline(df, relayout):
    timeframe = get_timeframe(relayout)
    if timeframe is None:
        filtered_df = df
    else:
        starttime, endtime = timeframe
        filtered_df = df.query('time <= @endtime and time >= @starttime')
    return filtered_df


@callback(
    Output('frequent-layers', 'figure'),
    Input('overall-timeline', 'relayoutData')
)
def create_frequent_layers(relayout):
    df = load_file(MATCH_FILE).query('live == True and player_count >= 30')
    filtered_df = filter_df_for_timeline(df, relayout)
    grouped_df = filtered_df[['previous_layer', 'minutes']].groupby('previous_layer').agg(['count', 'mean'])
    grouped_df.columns = grouped_df.columns.droplevel()
    grouped_df = grouped_df.reset_index()
    grouped_df['map_name'] = grouped_df['previous_layer'].apply(lambda x: x.split('_')[0])
    grouped_df['neg_count'] = -grouped_df['count']
    grouped_df = grouped_df.sort_values(by=['neg_count', 'previous_layer'])
    frequency_threshold = grouped_df.iloc[5]['count'] if len(grouped_df) >= 6 else 0
    top_entries_df = grouped_df.query('count >= @frequency_threshold')
    pie_color_map = get_map_color_palette('0')
    _fig = go.Figure(go.Bar(x=top_entries_df['previous_layer'], y=top_entries_df['count']))
    fig = px.bar(
        top_entries_df,
        x='previous_layer',
        category_orders={'previous_layer': [grouped_df['previous_layer']]},
        y='count',
        color='map_name',
        color_discrete_map=pie_color_map,
        title='Most frequent layers played',
        labels={'previous_layer': 'Layer'})

    integer_tick_values = list(range(1, max(top_entries_df['count'])+1, 1)) if len(grouped_df) != 0 else []
    fig.update_layout(
        xaxis={'categoryorder': 'array', 'categoryarray': top_entries_df['previous_layer']},
        yaxis={'tickvals': integer_tick_values}
    )

    return fig


@callback(
    Output('first-row-piechart', 'figure'),
    Output('match-table', 'children'),
    Input('overall-timeline', 'relayoutData')
)
def create_piecharts(relayout):
    df = load_file(MATCH_FILE).query('live == True and player_count >= 30')
    filtered_df = filter_df_for_timeline(df, relayout)

    grouped_df = filtered_df[['map_name', 'hours']].groupby('map_name').agg(['count', 'sum', 'mean'])
    grouped_df.columns = grouped_df.columns.droplevel()
    grouped_df = grouped_df.reset_index()
    grouped_df['sum'] = grouped_df['sum'].apply(lambda n: round(n, 2))

    version_df = filtered_df[['map_name', 'previous_layer', 'version']] \
        .groupby(['map_name', 'previous_layer']) \
        .agg(['count'])

    version_dict_raw = version_df.to_dict('index')
    version_dict = defaultdict(list)
    for (map_name, layer), count_dict in version_dict_raw.items():
        count = count_dict[('version', 'count')]
        version_dict[map_name].append(f'{layer}: {count}')
    version_information = ['<br>'.join(sorted(version_dict[map_name])) for map_name in grouped_df['map_name']]

    gamemode_df = filtered_df[['game_mode', 'hours']].groupby('game_mode').agg(['count', 'sum', 'mean'])
    gamemode_df.columns = gamemode_df.columns.droplevel()
    gamemode_df = gamemode_df.reset_index()
    pie_color_map = get_map_color_palette('0')
    style_data = dict(direction='clockwise',
                      textinfo='label+value',
                      textposition='inside',
                      insidetextorientation='radial',
                      sort=True, )

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
        customdata=version_information,
        domain=dict(x=[0, 1 / 3]),
        hovertemplate='%{label}<br>Times played: %{value} (%{percent})<br>Layers:<br>%{customdata}',
        marker_colors=[pie_color_map[mapname] for mapname in grouped_df['map_name']],
        **style_data
    ), row=1, col=1)
    fig.add_trace(go.Pie(
        name='',
        title='Hours spent on maps total',
        labels=grouped_df['map_name'],
        values=grouped_df['sum'],
        customdata=grouped_df['mean']*60,
        domain=dict(x=[1 / 3, 2 / 3]),
        hovertemplate='%{label}<br>Total hours: %{value:.2f} (%{percent})<br>Average length: %{customdata:.0f} mins',
        marker_colors=[pie_color_map[mapname] for mapname in grouped_df['map_name']],
        **style_data
    ), row=1, col=2)
    fig.add_trace(go.Pie(
        name='',
        title='Number of times a gamemode is played',
        labels=gamemode_df['game_mode'],
        values=gamemode_df['count'],
        domain=dict(x=[2 / 3, 1]),
        hovertemplate='%{label}<br>Frequency: %{value} (%{percent})',
        hovertext=gamemode_df['game_mode'],
        marker_colors=px.colors.qualitative.Dark24_r,
        **style_data
    ), row=1, col=3)
    fig.update_layout(legend=dict(orientation='h'), margin=dict(t=0))
    pretty_df_for_table = filtered_df.copy()
    pretty_df_for_table = pretty_df_for_table[['time', 'previous_layer', 'minutes', 'map_name', 'version']] \
        .rename({'previous_layer': 'Layer'}, axis=1)
    pretty_df_for_table['minutes'] = pretty_df_for_table['minutes'].apply(lambda m: round(m, 2))
    table = dash_table.DataTable(
        pretty_df_for_table.sort_values('time', ascending=False).to_dict('records'),
        [{"name": i, "id": i} for i in pretty_df_for_table.columns],
        page_size=10,
        style_table={'overflowX': 'auto', 'minWidth': '200px', 'width': '700px', 'maxWidth': '700px'},
    )
    return fig, table


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
                dict(count=1, label="1h", step="hour", stepmode="backward"),
                dict(count=6, label="6h", step="hour", stepmode="backward"),
                dict(count=1, label="1d", step="day", stepmode="backward"),
                dict(count=3, label="3d", step="day", stepmode="backward"),
                dict(count=7, label="1w", step="day", stepmode="backward"),
                dict(count=14, label="2w", step="day", stepmode="backward"),
                dict(count=1, label="1M", step="month", stepmode="backward"),
                dict(step="all")
            ])
        )
    )
    fig.update_yaxes(fixedrange=True)
    return fig


@callback(
    Output('seed-timeline', 'figure'),
    Output('server-status', 'children'),
    Input('load-interval', 'n_intervals'),
)
def create_seed_live_charts(_n_intervals: int):
    interesting_events = {'seed', 'live'}
    df = load_file(SEED_LIVE_FILE).copy()
    server_status = df.iloc[-1]['event']
    fig = px.bar(df.query('previous_event in @interesting_events'), x='date', y='hours', color='previous_event',
                 barmode='group',
                 color_discrete_sequence=px.colors.qualitative.Dark24_r,
                 title='How long the server is seeding and live daily', text='hours', text_auto='.2f',
                 labels={'previous_event': 'Event', 'seed': 'Seeding', 'live': 'Live'})
    fig.update_traces(textposition="inside", cliponaxis=False)
    fig.update_xaxes(tickformat='%d %B (%a)')
    return fig, [html.Span('Server is currently: '), html.B(pretty_events[server_status], style={'font-size': 19})]


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
