import plotly.express as px
from dash import dcc, Dash, html, Input, Output, callback
from plotly.graph_objs import Figure
import os
import pandas as pd

weather_df = pd.read_parquet('/app/data/Weather.parquet')
weather_df['Year'] = weather_df['day'].dt.year
weather_df['Month_number'] = weather_df['day'].dt.month
weather_df['Quarter'] = pd.to_datetime(weather_df['day']).dt.to_period('Q').astype(str)
weather_df['Month'] = pd.to_datetime(weather_df['day']).dt.to_period('M').astype(str)

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

def filter_data(df: pd.DataFrame, segment:list, category:str) -> pd.DataFrame:
    query = []
    if category:
        query.append(f'category_name == "{category}"')
    if segment:
        query.append(f'item_segment in {segment}')
    return df.query(" and ".join(query)) if query else df

def dash_layout (app: Dash) -> None:
    app.layout = html.Div([
        html.Div(children=[
            html.Img(src='/app/assets/D_logo.svg',
                     className='logo',),
            html.H5(children='By Daniel Echeverri Pérez', style={'color': '#ffff'}),
            ], className='grid-container'),
    ])

def run():
    app = Dash(__name__, title='Weather', external_stylesheets=[
    "https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;700&display=swap"])
    dash_layout(app)
    app.run(debug=True)

if __name__ == '__main__':
    run()