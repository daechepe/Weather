import plotly.express as px
from dash import dcc, Dash, html, Input, Output, callback
from plotly.graph_objs import Figure
import os
import pandas as pd

weather_df = pd.read_parquet('/app/data/Weather.parquet', engine="pyarrow")
weather_df['day'] = pd.to_datetime(weather_df['day'])
weather_df['Year'] = weather_df['day'].dt.year
weather_df['Month_number'] = weather_df['day'].dt.month
weather_df['Quarter'] = pd.to_datetime(weather_df['day']).dt.to_period('Q').astype(str)
weather_df['Month'] = pd.to_datetime(weather_df['day']).dt.to_period('M').astype(str)

def filter_data(df: pd.DataFrame, selected_city, selected_season) -> pd.DataFrame:
    if selected_city:
        df = df[df["city"].isin(selected_city)]
    if selected_season:
        df = df[df["season"] == selected_season]
    return df

def chart_layout(fig: Figure) -> Figure:
    return fig.update_layout(
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
        font=dict(color="#0d4753"),  # Change text color to contrast
        xaxis=dict(
            gridcolor='#74B2C1',  # Change grid color
            showgrid=True
        ),
        yaxis=dict(
            gridcolor="#74B2C1",
            showgrid=True
        ),
        title={
            'x': 0.5,
            'xanchor': 'center'
        }
    )

@callback(
    Output("total_metrics", "figure"),
    Input("item_city", "value"),
    Input("item_season", "value")
)
def update_temperature_plot(selected_city, selected_season):
    df_filtered = filter_data(weather_df.copy(), selected_city, selected_season)
    fig = px.line(
        df_filtered.groupby(['city', 'Month']).mean(numeric_only=True).reset_index(),
        x='Month',
        y='temperature_2m',
        title='Average Temperature by Month',
        color='city',
        markers=True,
    )
    return chart_layout(fig)

@callback(
    Output("graph", "figure"),
    Input("x-axis", "value"),
    Input("y-axis", "value"),
    Input("item_city", "value"),
    Input("item_season", "value")
)
def generate_chart(x, y, selected_city, selected_season):
    df_filtered = filter_data(weather_df.copy(), selected_city, selected_season)
    fig = px.box(df_filtered, x=x, y=y)
    return fig

@callback(
    Output("hist", "figure"),
    Input("x-axis_hist", "value"),
    Input("item_city", "value"),
    Input("item_season", "value")
)
def generate_hist_chart(x, selected_city, selected_season):
    df_filtered = filter_data(weather_df.copy(), selected_city, selected_season)
    fig = px.histogram(df_filtered, x=x, color='city')
    return fig

def dash_layout (app: Dash) -> None:
    app.layout = html.Div([
        html.Div(children=[
            html.Img(
                src='./assets/D_logo_neg.svg',
                className='logo', ),
            html.H5(children='By Daniel Echeverri Pérez', style={'color': '#ffff'}),
        ], className='grid-container'),
        html.Div(children=[
            html.Div(children=[
                html.Label('City'),
                dcc.Dropdown(id='item_city', options=weather_df['city'].unique().tolist(),
                             placeholder='Select City',  className="grid-item_dropdown", multi=True),
            ], className="grid-item"),
            html.Div(children=[
                html.Label('Season'),
                dcc.Dropdown(id='item_season', options=weather_df['season'].unique().tolist(),
                             placeholder='Select Season', className="grid-item_dropdown"),
            ], className="grid-item"),
        ], className='grid-container'),

        html.Div(children=[
            dcc.Graph(id='total_metrics', figure={}),

            html.Div(children=[
                html.Div(children=[
                    html.Label('y-axis'),
                    dcc.Dropdown(id='y-axis', options=['temperature_2m', 'rain', 'humidity'],
                                 value='temperature_2m', className="grid-item_dropdown"),
                ], className="grid-item"),
                html.Div(children=[
                    html.Label('x-axis'),
                    dcc.Dropdown(id='x-axis', options=['season', 'Month', 'climate', 'zone', 'city'],
                                 value='season', className="grid-item_dropdown"),
                ], className="grid-item"),
            ], className='grid-container'),

            dcc.Graph(id='graph', figure={}),

            html.Div(children=[
                html.Div(children=[
                    html.Label('x-axis'),
                    dcc.Dropdown(id='x-axis_hist', options=['temperature_2m', 'rain', 'humidity'],
                                 value='temperature_2m', className="grid-item_dropdown"),
                ], className="grid-item"),
            ], className='grid-container'),

            dcc.Graph(id='hist', figure={}),
        ]),
    ])

def run():
    app = Dash(__name__, title='Weather', external_stylesheets=[
    "https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;700&display=swap"],assets_folder='./assets')
    dash_layout(app)
    app.run(host='0.0.0.0', port=8050, debug=True)

if __name__ == '__main__':
    run()