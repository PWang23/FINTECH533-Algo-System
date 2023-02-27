from dash import Dash, html, dcc, dash_table, Input, Output, State
import dash_bootstrap_components as dbc
import eikon as ek
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import plotly.express as px
import os

ek.set_app_key(os.getenv('AppKey'))

app = Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])

percentage = dash_table.FormatTemplate.percentage(3)

controls = dbc.Card(
    [
        dbc.Row(html.Button('QUERY Refinitiv', id='run-query', n_clicks=0)),
        dbc.Row(
            dbc.Table(
                [
                    html.Thead(
                        html.Tr([html.Th("Benchmark ID"), html.Th("Asset ID")])
                    ),
                    html.Tbody([
                        html.Tr([
                            html.Td(
                                dbc.Input(
                                    id='benchmark-id',
                                    type='text',
                                    value="IVV"
                                )
                            ),
                            html.Td(
                                dbc.Input(
                                    id='asset-id',
                                    type='text',
                                    value="AAPL.O"
                                )
                            )
                        ])
                    ])
                ],
                bordered=True
            )
        ),
        dbc.Row([
            dcc.DatePickerRange(
                id='refinitiv-date-range',
                min_date_allowed = date(2015, 1, 1),
                max_date_allowed = datetime.now(),
                start_date = datetime.date(
                    datetime.now() - timedelta(days=3*365)
                ),
                end_date = datetime.now().date()
            )
        ])
    ],
    body=True
)

app.layout = dbc.Container(
    [
        dbc.Row(
            [
                dbc.Col(controls, md=4),
                dbc.Col(
                    [
                        dcc.Graph(id="ab-plot"),
                        dcc.RangeSlider(
                            id='ab-range-slider',
                            min = 0,
                            max = 25,
                            marks = None,
                            step = 1
                        )
                    ],
                    md = 8,
                    style= {'display': 'block'}
                ),
            ],
            align="center",
        ),
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
        )
    ],
    fluid=True
)

@app.callback(
    Output("history-tbl", "data"),
    Input("run-query", "n_clicks"),
    [
        State('benchmark-id', 'value'),
        State('asset-id', 'value'),
        State('refinitiv-date-range', 'start_date'),
        State('refinitiv-date-range', 'end_date')
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
    prices.dropna(inplace=True)

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
    divs.dropna(inplace=True)

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
    splits.dropna(inplace=True)

    #######

    prices.rename(
        columns={
            'Open Price': 'open',
            'High Price': 'high',
            'Low Price': 'low',
            'Close Price': 'close'
        },
        inplace=True
    )
    prices['Date'] = pd.to_datetime(prices['Date']).dt.date

    if divs.shape[0] == 0:
        unadjusted_price_history = prices
        unadjusted_price_history['div_amt'] = 0
    else:
        divs.rename(
            columns={
                'Dividend Ex Date': 'Date',
                'Gross Dividend Amount': 'div_amt',
                'Dividend Type': 'div_type',
                'Dividend Payment Type': 'pay_type'
            },
            inplace=True
        )
        divs['Date'] = pd.to_datetime(divs['Date']).dt.date
        divs = divs[(divs.Date.notnull()) & (divs.div_amt > 0)]
        divs = divs.groupby(['Instrument', 'Date'], as_index=False).agg({
            'div_amt': 'sum',
            'div_type': lambda x: ", ".join(x),
            'pay_type': lambda x: ", ".join(x)
        })
        unadjusted_price_history = pd.merge(
            prices, divs[['Instrument', 'Date', 'div_amt']],
            how='outer',
            on=['Date', 'Instrument']
        )
        unadjusted_price_history['div_amt'].fillna(0, inplace=True)

    if splits.shape[0] == 0:
        unadjusted_price_history = prices
        unadjusted_price_history['split_rto'] = 1
    else:
        splits.rename(
            columns={
                'Capital Change Effective Date': 'Date',
                'Adjustment Factor': 'split_rto'
            },
            inplace=True
        )
        splits['Date'] = pd.to_datetime(splits['Date']).dt.date

        unadjusted_price_history = pd.merge(
            unadjusted_price_history, splits,
            how='outer',
            on=['Date', 'Instrument']
        )
        unadjusted_price_history['split_rto'].fillna(1, inplace=True)

    if unadjusted_price_history.isnull().values.any():
        raise Exception('missing values detected!')

    return(unadjusted_price_history.to_dict('records'))

@app.callback(
    [
        Output("returns-tbl", "data"),
        Output("returns-tbl", "columns"),
        Output('ab-range-slider', 'min'),
        Output('ab-range-slider', 'max'),
        Output('ab-range-slider', 'value')
    ],
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

    hist_rtns = pd.DataFrame({
        'Date': numerator[dte_col].reset_index(drop=True),
        'Instrument': numerator[ins_col].reset_index(drop=True),
        'rtn': np.log(
            (numerator[prc_col] + numerator[div_col]).reset_index(drop=True) / (
                    denominator[prc_col] * denominator[spt_col]
            ).reset_index(drop=True)
        )
    }).pivot_table(
        values='rtn', index='Date', columns='Instrument'
    )

    hist_rtns.reset_index(inplace=True)
    hist_rtns = hist_rtns.rename(columns={'index': 'Date'})
    hist_rtns['Date'] = pd.to_datetime(hist_rtns['Date']).dt.date

    columns = [
        dict(id=hist_rtns.columns[0], name=hist_rtns.columns[0]),
        dict(
            id=hist_rtns.columns[1],
            name=hist_rtns.columns[1],
            type='numeric',
            format=percentage
        ),
        dict(
            id=hist_rtns.columns[2],
            name=hist_rtns.columns[2],
            type='numeric',
            format=percentage
        ),
    ]

    return (
        hist_rtns.to_dict('records'), columns, 0, hist_rtns.shape[0],
        [0, hist_rtns.shape[0]]
    )

@app.callback(
    Output("ab-plot", "figure"),
    [Input("returns-tbl", "data"), Input('ab-range-slider', 'value')],
    prevent_initial_call = True
)
def render_ab_plot(returns, slider_range):

    returns = pd.DataFrame(returns)
    returns = returns[int(slider_range[0]):int(slider_range[1])]

    fig = px.scatter(
        returns,
        x=returns.columns[1],
        y=returns.columns[2],
        trendline='ols'
    )

    fit_results = px.get_trendline_results(fig).px_fit_results.iloc[0].params

    fig.update_layout(
        title = "Benchmark Plot: " + returns.columns[2] + " vs " + \
                returns.columns[1] + '<br><sup>' + "Alpha: " + \
                str("{:.5%}".format(fit_results[0])) + "; Beta: " + \
                str(round(fit_results[1], 3)) + '     From ' + \
                returns['Date'][slider_range[0]] + ' to ' + returns['Date'][
                    slider_range[0]] + '</sup>',
        xaxis=dict(tickformat=".2%"),
        yaxis=dict(tickformat=".2%")
    )

    return(fig)

if __name__ == '__main__':
    app.run_server(debug=True)