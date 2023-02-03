from dash import Dash, html, dcc, dash_table, Input, Output, State
import eikon as ek
import pandas as pd
import numpy as np
from datetime import datetime, date
import plotly.express as px
import os

ek.set_app_key(os.getenv('AppKey'))

#ek.set_app_key('977aeb771744454e8803c10c8704c8e1ef2f4c27')

#not used yet, only small amount of data in this file
dt_prc_div_splt = pd.read_csv('unadjusted_price_history.csv')

app = Dash(__name__)
app.layout = html.Div([
    html.Div([
        html.Label('benchmark:  '),
        dcc.Input(id = 'benchmark-id', type = 'text', value="IVV"),
        html.Label('asset:  '),
        dcc.Input(id = 'asset-id', type = 'text', value="AAPL.O")
    ]),
    html.Div([
        html.Label('Raw data filter'),
        dcc.DatePickerRange(
        id='raw-data-date-picker-range',
        display_format='YYYY-MM-DD',
        min_date_allowed=date(2015, 8, 5), 
        max_date_allowed=date.today(),
        initial_visible_month=date(2019, 8, 5)
    )
    ]),
    html.Button('QUERY Refinitiv', id = 'run-query', n_clicks = 0),
    html.H2('Raw Data from Refinitiv'),
    dash_table.DataTable(
        id = "history-tbl",
        page_action='none',
        style_table={'height': '300px', 'overflowY': 'auto'}
    ),
    html.H2('Historical Returns'),
    dash_table.DataTable(
        id = "returns-tbl",
        page_action='none',
        style_table={'height': '300px', 'overflowY': 'auto'}
    ),
    html.H2('Alpha & Beta Scatter Plot'),
    html.Div([
        html.Label('Plot data filter'),
        dcc.DatePickerRange(
        id='plot-date-picker-range',
        display_format='YYYY-MM-DD',
        min_date_allowed=date(2015, 8, 5), 
        max_date_allowed=date.today(),
        initial_visible_month=date(2019, 8, 5)
    )
    ]),
    html.Button('Plot', id = 'abPlot', n_clicks = 0),
    dcc.Graph(id="ab-plot"),
    html.P(id='summary-text', children="")
])

@app.callback(
    Output('plot-date-picker-range', 'min_date_allowed'),
    Output('plot-date-picker-range', 'max_date_allowed'),
    Output('plot-date-picker-range', 'initial_visible_month'),
    [Input('raw-data-date-picker-range', 'start_date'),
    Input('raw-data-date-picker-range', 'end_date')
    ],
    prevent_initial_call=True
)
def update_plot_date_range(start_date, end_date):
    start_day_plus1 = pd.to_datetime(start_date) + pd.Timedelta(days=1)
    return start_day_plus1.strftime("%Y-%m-%d"), end_date, start_day_plus1.strftime("%Y-%m-%d")

@app.callback(
    Output("history-tbl", "data"),
    Input("run-query", "n_clicks"),
    [State('benchmark-id', 'value'),
    State('asset-id', 'value'),
    State('raw-data-date-picker-range', 'start_date'),
    State('raw-data-date-picker-range', 'end_date')
    ],
    prevent_initial_call=True
)
def query_refinitiv(n_clicks, benchmark_id, asset_id, start_date, end_date):
    assets = [benchmark_id, asset_id]
    prices, prc_err = ek.get_data(
        instruments=assets,
        fields=[
            'TR.OPENPRICE(Adjusted=0)',
            'TR.HIGHPRICE(Adjusted=0)',
            'TR.LOWPRICE(Adjusted=0)',
            'TR.CLOSEPRICE(Adjusted=0)',
            'TR.PriceCloseDate'
        ],
        parameters={
            'SDate': start_date,
            'EDate': end_date,
            'Frq': 'D'
        }
    )

    divs, div_err = ek.get_data(
        instruments=assets,
        fields=[
            'TR.DivExDate',
            'TR.DivUnadjustedGross',
            'TR.DivType',
            'TR.DivPaymentType'
        ],
        parameters={
            'SDate': start_date,
            'EDate': end_date,
            'Frq': 'D'
        }
    )

    splits, splits_err = ek.get_data(
        instruments=assets,
        fields=['TR.CAEffectiveDate', 'TR.CAAdjustmentFactor'],
        parameters={
            "CAEventType": "SSP",
            'SDate': start_date,
            'EDate': end_date,
            'Frq': 'D'
        }
    )

    prices.rename(
        columns={
            'Open Price': 'open',
            'High Price': 'high',
            'Low Price': 'low',
            'Close Price': 'close'
        },
        inplace=True
    )
    prices.dropna(inplace=True)
    prices['Date'] = pd.to_datetime(prices['Date']).dt.date

    divs.rename(
        columns={
            'Dividend Ex Date': 'Date',
            'Gross Dividend Amount': 'div_amt',
            'Dividend Type': 'div_type',
            'Dividend Payment Type': 'pay_type'
        },
        inplace=True
    )
    divs.dropna(inplace=True)
    divs['Date'] = pd.to_datetime(divs['Date']).dt.date
    divs = divs[(divs.Date.notnull()) & (divs.div_amt > 0)]
    divs = divs.groupby(['Instrument', 'Date'], as_index = False).agg({
    'div_amt': 'sum',
    'div_type': lambda x: ", ".join(x),
    'pay_type': lambda x: ", ".join(x)
    })

    splits.rename(
        columns={
            'Capital Change Effective Date': 'Date',
            'Adjustment Factor': 'split_rto'
        },
        inplace=True
    )
    splits.dropna(inplace=True)
    splits['Date'] = pd.to_datetime(splits['Date']).dt.date

    unadjusted_price_history = pd.merge(
        prices, divs[['Instrument', 'Date', 'div_amt']],
        how='outer',
        on=['Date', 'Instrument']
    )
    unadjusted_price_history['div_amt'].fillna(0, inplace=True)

    unadjusted_price_history = pd.merge(
        unadjusted_price_history, splits,
        how='outer',
        on=['Date', 'Instrument']
    )
    unadjusted_price_history['split_rto'].fillna(1, inplace=True)

    if unadjusted_price_history.isnull().values.any():
        raise Exception('missing values detected!')

    unadjusted_price_history.drop_duplicates(inplace=True)

    return(unadjusted_price_history.to_dict('records'))

@app.callback(
    Output("returns-tbl", "data"),
    Input("history-tbl", "data"),
    prevent_initial_call = True
)
def calculate_returns(history_tbl):

    dt_prc_div_splt = pd.DataFrame(history_tbl)

    # Define what columns contain the Identifier, date, price, div, & split info
    ins_col = 'Instrument'
    dte_col = 'Date'
    prc_col = 'close'
    div_col = 'div_amt'
    spt_col = 'split_rto'

    dt_prc_div_splt[dte_col] = pd.to_datetime(dt_prc_div_splt[dte_col])
    dt_prc_div_splt = dt_prc_div_splt.sort_values([ins_col, dte_col])[
        [ins_col, dte_col, prc_col, div_col, spt_col]].groupby(ins_col)
    numerator = dt_prc_div_splt[[dte_col, ins_col, prc_col, div_col]].tail(-1)
    denominator = dt_prc_div_splt[[prc_col, spt_col]].head(-1)
    res = pd.DataFrame({
        'Date': numerator[dte_col].reset_index(drop=True),
        'Instrument': numerator[ins_col].reset_index(drop=True),
        'rtn': np.log(
            (numerator[prc_col] + numerator[div_col]).reset_index(drop=True) / (
                    denominator[prc_col] * denominator[spt_col]
            ).reset_index(drop=True)
        )}).pivot_table(
            values='rtn', index='Date', columns='Instrument'
        ).reset_index()
    res['Date'] = pd.to_datetime(res['Date']).dt.date

    return(
        res.to_dict('records')
    )

@app.callback(
    Output("ab-plot", "figure"),
    Output("summary-text", "children"),
    Input("abPlot", "n_clicks"),
    [State("returns-tbl", "data"),
    State('benchmark-id', 'value'),
    State('asset-id', 'value'),
    State('plot-date-picker-range', 'start_date'),
    State('plot-date-picker-range', 'end_date')
    ],
    prevent_initial_call = True
)
def render_ab_plot(n_clicks,returns, benchmark_id, asset_id, start_date, end_date):
    returns_df = pd.DataFrame(returns)
    returns_df['Date'] = pd.to_datetime(returns_df['Date']).dt.date
    date_format = "%Y-%m-%d"
    start_date = datetime.strptime(start_date, date_format).date()
    end_date = datetime.strptime(end_date, date_format).date()
    returns_df = returns_df[(returns_df['Date'] >= start_date) & (returns_df['Date'] <= end_date)]
    returns = returns_df.to_dict()
    fig = px.scatter(returns, x=benchmark_id, y=asset_id, trendline='ols')
    model = px.get_trendline_results(fig)
    alpha = model.iloc[0]["px_fit_results"].params[0]
    beta = model.iloc[0]["px_fit_results"].params[1]
    alpha_beta_string = 'alpha is ' + str(alpha) + ', beta is ' + str(beta)
    return(fig, alpha_beta_string)

if __name__ == '__main__':
    app.run_server(debug=True, port=8888)