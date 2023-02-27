from dash import Dash, html, dcc, dash_table, Input, Output, State
import dash_bootstrap_components as dbc
import eikon as ek
import pandas as pd
import numpy as np
from datetime import date, datetime
import refinitiv.data as rd

import os

ek.set_app_key(os.getenv('AppKey'))


app = Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])

percentage = dash_table.FormatTemplate.percentage(3)

controls = dbc.Card(
    [
        dbc.Row(html.Button('QUERY Refinitiv', id='run-query', n_clicks=0)),
        dbc.Row([
            html.H5('Asset:',
                    style={'display': 'inline-block', 'margin-right': 20}),
            dcc.Input(id='asset', type='text', value="IVV",
                      style={'display': 'inline-block',
                             'border': '1px solid black'}),
            dbc.Table(
                [
                    html.Thead(html.Tr([html.Th(["\u03B1", html.Sub("1")]), html.Th("n1")])),
                    html.Tbody([
                        html.Tr([
                            html.Td(
                                dbc.Input(
                                    id='alpha1',
                                    type='number',
                                    value=-0.01,
                                    max=1,
                                    min=-1,
                                    step=0.01
                                )
                            ),
                            html.Td(
                                dcc.Input(
                                    id='n1',
                                    type='number',
                                    value=3,
                                    min=1,
                                    step=1
                                )
                            )
                        ])
                    ])
                ],
                bordered=True
            ),
            dbc.Table(
                [
                    html.Thead(html.Tr([html.Th(["\u03B1", html.Sub("2")]), html.Th("n2")])),
                    html.Tbody([
                        html.Tr([
                            html.Td(
                                dbc.Input(
                                    id='alpha2',
                                    type='number',
                                    value=0.01,
                                    max=1,
                                    min=-1,
                                    step=0.01
                                )
                            ),
                            html.Td(
                                dcc.Input(
                                    id='n2',
                                    type='number',
                                    value=5,
                                    min=1,
                                    step=1
                                )
                            )
                        ])
                    ])
                ],
                bordered=True
            )
        ]),
        dbc.Row([
            dcc.DatePickerRange(
                id='refinitiv-date-range',
                min_date_allowed = date(2015, 1, 1),
                max_date_allowed = datetime.now(),
                start_date = date(2023, 1, 30),
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
                    # Put your reactive graph here as an image!
                    md = 8
                )
            ],
            align="center",
        ),
        html.H2('Trade Blotter:'),
        dash_table.DataTable(
            id="history-tbl",
            style_table={'display': 'none'}
        ),
        dash_table.DataTable(
            id="entry-tbl",
            style_table={'display': 'none'}
        ),
        dash_table.DataTable(
            id="exit-tbl",
            style_table={'display': 'none'}
        ),
        dash_table.DataTable(id = "blotter"),
        html.Footer('Copyright © Qihang Ma, Yuanzhe Wang')
    ],
    fluid=True
)


@app.callback(
    Output("history-tbl", "data"),
    Input("run-query", "n_clicks"),
    [State('asset', 'value'),
     State('refinitiv-date-range', 'start_date'), State('refinitiv-date-range', 'end_date')],
    prevent_initial_call=True
)
def query_refinitiv(n_clicks, asset_id, start_date, end_date):
    assets = [asset_id]
    start_date_object = date.fromisoformat(start_date)
    end_date_object = date.fromisoformat(end_date)
    data_start = start_date_object.strftime("%Y-%m-%d")
    data_end = end_date_object.strftime("%Y-%m-%d")
    prices, prc_err = ek.get_data(
        instruments=assets,
        fields = [
            'TR.OPENPRICE(Adjusted=0)',
            'TR.HIGHPRICE(Adjusted=0)',
            'TR.LOWPRICE(Adjusted=0)',
            'TR.CLOSEPRICE(Adjusted=0)',
            'TR.PriceCloseDate'
        ],
        parameters = {
            'SDate': data_start,
            'EDate': data_end,
            'Frq': 'D'
        }
    )

    prices['Date'] = pd.to_datetime(prices['Date']).dt.date
    prices.drop(columns='Instrument', inplace=True)

    return(prices.to_dict('records'))

@app.callback(
    Output("entry-tbl", "data"),
    Input("run-query", "n_clicks"),
    Input("history-tbl", "data"),
    Input('n1', 'value'),
    Input('alpha1', 'value'),
    State('asset', 'value'),
    prevent_initial_call = True
)
def get_entry_tbl(n_clicks, history_tbl, n1, alpha1, asset):
    n1 = int(n1)
    alpha1 = float(alpha1)
    prices = pd.DataFrame(history_tbl)
    prices['Date'] = list(pd.to_datetime(prices["Date"].iloc[:]).dt.date)
    ##### Get the next business day from Refinitiv!!!!!!!
    rd.open_session()

    next_business_day = rd.dates_and_calendars.add_periods(
        start_date= prices['Date'].iloc[-1].strftime("%Y-%m-%d"),
        period="1D",
        calendars=["USA"],
        date_moving_convention="NextBusinessDay",
    )

    rd.close_session()
    ######################################################

    submitted_entry_orders = pd.DataFrame({
        "trade_id": range(1, prices.shape[0]),
        "date": list(pd.to_datetime(prices["Date"].iloc[1:]).dt.date),
        "asset": asset,
        "trip": 'ENTER',
        "action": "BUY",
        "type": "LMT",
        "price": prices['Close Price'].iloc[:-1] * (1 + alpha1),
        'status': 'SUBMITTED'
    })
    #print(submitted_entry_orders)



    # if the lowest traded price is still higher than the price you bid, then the
    # order never filled and was cancelled.
    with np.errstate(invalid='ignore'):
        cancelled_entry_orders = submitted_entry_orders[
            np.greater(
                prices['Low Price'].iloc[1:][::-1].rolling(n1).min()[::-1].to_numpy(),
                submitted_entry_orders['price'].to_numpy()
            )
        ].copy()
    cancelled_entry_orders.reset_index(drop=True, inplace=True)
    cancelled_entry_orders['status'] = 'CANCELLED'
    cancelled_entry_orders['date'] = pd.DataFrame(
        {'cancel_date': submitted_entry_orders['date'].iloc[(n1-1):].to_numpy()},
        index=submitted_entry_orders['date'].iloc[:(1-n1)].to_numpy()
    ).loc[cancelled_entry_orders['date']]['cancel_date'].to_list()
    #print(cancelled_entry_orders)

    filled_entry_orders = submitted_entry_orders[
        submitted_entry_orders['trade_id'].isin(
            list(
                set(submitted_entry_orders['trade_id']) - set(
                    cancelled_entry_orders['trade_id']
                )
            )
        )
    ].copy()
    filled_entry_orders.reset_index(drop=True, inplace=True)
    filled_entry_orders['status'] = 'FILLED'
    for i in range(0, len(filled_entry_orders)):

        idx1 = np.flatnonzero(
            prices['Date'] == filled_entry_orders['date'].iloc[i]
        )[0]

        ivv_slice = prices.iloc[idx1:(idx1+n1)]['Low Price']

        fill_inds = ivv_slice <= filled_entry_orders['price'].iloc[i]

        if (len(fill_inds) < n1) & (not any(fill_inds)):
            filled_entry_orders.at[i, 'status'] = 'LIVE'
        else:
            filled_entry_orders.at[i, 'date'] = prices['Date'].iloc[
                fill_inds.idxmax()
            ]

    if any(filled_entry_orders['status'] =='LIVE'):
        live_entry_orders = pd.concat(
            [
                pd.DataFrame({
                    "trade_id": prices.shape[0],
                    "date": pd.to_datetime(next_business_day).date(),
                    "asset": asset,
                    "trip": 'ENTER',
                    "action": "BUY",
                    "type": "LMT",
                    "price": prices['Close Price'].iloc[-1] * (1 + alpha1),
                    'status': 'LIVE'
                },
                    index=[0]
                ),
                filled_entry_orders[filled_entry_orders['status'] == 'LIVE']
            ]
        )
    else:
        live_entry_orders = pd.DataFrame({
            "trade_id": prices.shape[0],
            "date": pd.to_datetime(next_business_day).date(),
            "asset": asset,
            "trip": 'ENTER',
            "action": "BUY",
            "type": "LMT",
            "price": prices['Close Price'].iloc[-1] * (1 + alpha1),
            'status': 'LIVE'
        },
            index=[0]
        )


    filled_entry_orders = filled_entry_orders[
        filled_entry_orders['status'] == 'FILLED'
        ]

    #print(filled_entry_orders)
    #print(live_entry_orders)

    entry_orders = pd.concat(
        [
            submitted_entry_orders,
            cancelled_entry_orders,
            filled_entry_orders,
            live_entry_orders
        ]
    ).sort_values(["date", 'trade_id'])

    return(entry_orders.to_dict('records'))


@app.callback(
    Output("exit-tbl", "data"),
    Input("run-query", "n_clicks"),
    Input("entry-tbl", "data"),
    Input("history-tbl", "data"),
    Input('n2', 'value'),
    Input('alpha2', 'value'),
    State('asset', 'value'),
    prevent_initial_call = True
)
def get_exit_tbl(n_clicks, entry_tbl, history_tbl, n2, alpha2, asset):    
    n2 = int(n2)
    alpha2 = float(alpha2)
    prices = pd.DataFrame(history_tbl)
    prices['Date'] = list(pd.to_datetime(prices["Date"].iloc[:]).dt.date)
    entry = pd.DataFrame(entry_tbl)
    entry['date'] = list(pd.to_datetime(entry["date"].iloc[:]).dt.date)

    filled_entry_orders = entry[entry['status']=='FILLED'].copy()

    submitted_exit_orders = pd.DataFrame({
        "trade_id": list(filled_entry_orders['trade_id']),
        "date": list(pd.to_datetime(filled_entry_orders["date"].iloc[:]).dt.date),
        "asset": asset,
        "trip": 'EXIT',
        "action": "SELL",
        "type": "LMT",
        "price": filled_entry_orders['price'].iloc[:] * (1 + alpha2),
        'status': 'SUBMITTED'
    })
    submitted_exit_orders = submitted_exit_orders.sort_values(["date", 'trade_id'])
    submitted_exit_orders.reset_index(drop=True, inplace=True)


    high_prices = pd.DataFrame({
        'High_price': prices['High Price'][::-1].rolling(window=n2).apply(lambda x: x[:-1].max(), raw=True)[::-1],
        'date': list(pd.to_datetime(prices["Date"].iloc[:]).dt.date),
        'Close_price':prices['Close Price']
        })
    
    high_prices['High_price'] = high_prices['High_price'].where(high_prices['High_price'].isna() | (high_prices['Close_price'] < high_prices['High_price']), high_prices['Close_price'])
    

    merge_orders =  pd.merge(submitted_exit_orders, high_prices, on='date', how='left')
    merge_orders.drop_duplicates(subset=['trade_id'], inplace=True)
    
    with np.errstate(invalid='ignore'):
        cancelled_exit_orders = submitted_exit_orders[
            np.greater(
                submitted_exit_orders['price'].to_numpy(),
                merge_orders['High_price'].to_numpy()
            )
        ].copy()
    cancelled_exit_orders.reset_index(drop=True, inplace=True)
    cancelled_exit_orders['status'] = 'CANCELLED'

    cancelled_exit_orders['date'] = pd.DataFrame(
        {'cancel_date': prices['Date'].iloc[(n2-1):].to_numpy()},
        index=prices['Date'].iloc[:(1-n2)].to_numpy()
    ).loc[cancelled_exit_orders['date']]['cancel_date'].to_list()


    filled_exit_orders = submitted_exit_orders[
        submitted_exit_orders['trade_id'].isin(
            list(
                set(submitted_exit_orders['trade_id']) - set(
                    cancelled_exit_orders['trade_id']
                )
            )
        )
    ].copy()
    filled_exit_orders.reset_index(drop=True, inplace=True)
    filled_exit_orders['status'] = 'FILLED'

    for i in range(0, len(filled_exit_orders)):

        idx1 = np.flatnonzero(
            prices['Date'] == filled_exit_orders['date'].iloc[i]
        )[0]

        #price_slice = pd.Series([prices.iloc[idx1]['Close Price']] + list(prices.iloc[idx1+1:(idx1+n2)]['High Price']))
        price_slice = prices.iloc[idx1:(idx1+n2)]['High Price']

        fill_inds = price_slice >= filled_exit_orders['price'].iloc[i]

        if (len(fill_inds) < n2) & (not any(fill_inds)):
            filled_exit_orders.at[i, 'status'] = 'LIVE'
        else:
            filled_exit_orders.at[i, 'date'] = prices['Date'].iloc[
                fill_inds.idxmax()
            ]
    
    live_exit_orders = filled_exit_orders[
        filled_exit_orders['status'] == 'LIVE'
        ]
    
    for i in range(0, len(live_exit_orders)):
        live_exit_orders['date'] = list(prices['Date'])[-1]

    filled_exit_orders = filled_exit_orders[
        filled_exit_orders['status'] == 'FILLED'
        ]

    market_exit_orders = pd.DataFrame({
        "trade_id": list(cancelled_exit_orders['trade_id']),
        "date": list(pd.to_datetime(cancelled_exit_orders["date"].iloc[:]).dt.date),
        "asset": asset,
        "trip": 'EXIT',
        "action": "SELL",
        "type": "MKT",
        "price": list(cancelled_exit_orders['price']),
        'status': 'FILLED'
    })

    for i in range(0, len(market_exit_orders)):

        idx1 = np.flatnonzero(
            prices['Date'] == market_exit_orders['date'].iloc[i]
        )[0]

        market_exit_orders.loc[i, 'price'] = prices.loc[idx1, 'Close Price']


    exit_orders = pd.concat(
        [
            submitted_exit_orders,
            cancelled_exit_orders,
            filled_exit_orders,
            live_exit_orders,
            market_exit_orders
        ]
    ).sort_values(["date", 'trade_id'])

    return(exit_orders.to_dict('records'))


@app.callback(
    Output("blotter", "data"),
    Input("run-query", "n_clicks"),
    Input("entry-tbl", "data"),
    Input('exit-tbl', 'data'),
    prevent_initial_call = True
)
def get_blotter(n_clicks, entry_tbl, exit_tbl):
    entry_tbl = pd.DataFrame(entry_tbl)
    exit_tbl = pd.DataFrame(exit_tbl)    
    result = pd.concat([entry_tbl, exit_tbl]).sort_values(['date', 'trade_id'])
    return(result.to_dict('records'))

if __name__ == '__main__':
    app.run_server(debug=True)
