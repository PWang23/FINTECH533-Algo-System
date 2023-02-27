import pandas as pd
from datetime import datetime
import numpy as np
import os
import eikon as ek
import refinitiv.data as rd
import time

""" start_time = time.time() """

#####################################################

ek.set_app_key(os.getenv('AppKey'))

ivv_prc, ivv_prc_err = ek.get_data(
    instruments = ["IVV"],
    fields = [
        'TR.OPENPRICE(Adjusted=0)',
        'TR.HIGHPRICE(Adjusted=0)',
        'TR.LOWPRICE(Adjusted=0)',
        'TR.CLOSEPRICE(Adjusted=0)',
        'TR.PriceCloseDate'
    ],
    parameters = {
        'SDate': '2023-01-30',
        'EDate': '2023-02-08',
        'Frq': 'D'
    }
)




ivv_prc['Date'] = pd.to_datetime(ivv_prc['Date']).dt.date
ivv_prc.drop(columns='Instrument', inplace=True)

##### Get the next business day from Refinitiv!!!!!!!
rd.open_session()

next_business_day = rd.dates_and_calendars.add_periods(
    start_date= ivv_prc['Date'].iloc[-1].strftime("%Y-%m-%d"),
    period="1D",
    calendars=["USA"],
    date_moving_convention="NextBusinessDay",
)

rd.close_session()
######################################################

# Parameters:
alpha1 = -0.01
n1 = 3

# submitted entry orders
submitted_entry_orders = pd.DataFrame({
    "trade_id": range(1, ivv_prc.shape[0]),
    "date": list(pd.to_datetime(ivv_prc["Date"].iloc[1:]).dt.date),
    "asset": "IVV",
    "trip": 'ENTER',
    "action": "BUY",
    "type": "LMT",
    "price": ivv_prc['Close Price'].iloc[:-1] * (1 + alpha1),
    'status': 'SUBMITTED'
})
#print(submitted_entry_orders)



# if the lowest traded price is still higher than the price you bid, then the
# order never filled and was cancelled.
with np.errstate(invalid='ignore'):
    cancelled_entry_orders = submitted_entry_orders[
        np.greater(
            ivv_prc['Low Price'].iloc[1:][::-1].rolling(n1).min()[::-1].to_numpy(),
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
        ivv_prc['Date'] == filled_entry_orders['date'].iloc[i]
    )[0]

    ivv_slice = ivv_prc.iloc[idx1:(idx1+n1)]['Low Price']

    fill_inds = ivv_slice <= filled_entry_orders['price'].iloc[i]

    if (len(fill_inds) < n1) & (not any(fill_inds)):
        filled_entry_orders.at[i, 'status'] = 'LIVE'
    else:
        filled_entry_orders.at[i, 'date'] = ivv_prc['Date'].iloc[
            fill_inds.idxmax()
        ]

""" if any(filled_entry_orders['status'] =='LIVE'):
    live_entry_orders = pd.concat(
        [
            pd.DataFrame({
                "trade_id": ivv_prc.shape[0],
                "date": pd.to_datetime(next_business_day).date(),
                "asset": "IVV",
                "trip": 'ENTER',
                "action": "BUY",
                "type": "LMT",
                "price": ivv_prc['Close Price'].iloc[-1] * (1 + alpha1),
                'status': 'LIVE'
            },
                index=[0]
            ),
            filled_entry_orders[filled_entry_orders['status'] == 'LIVE']
        ]
    )
else:
    live_entry_orders = pd.DataFrame({
        "trade_id": ivv_prc.shape[0],
        "date": pd.to_datetime(next_business_day).date(),
        "asset": "IVV",
        "trip": 'ENTER',
        "action": "BUY",
        "type": "LMT",
        "price": ivv_prc['Close Price'].iloc[-1] * (1 + alpha1),
        'status': 'LIVE'
    },
        index=[0]
    ) """

live_entry_orders = filled_entry_orders[
        filled_entry_orders['status'] == 'LIVE'
        ]

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

#print(entry_orders)

## Exit orders
alpha2 = 0.01
n2 = 5


## Submit limit sell orders
submitted_exit_orders = pd.DataFrame({
	#"trade_id": list(filled_entry_orders['trade_id']),
	"trade_id": range(1, filled_entry_orders.shape[0]+1),
    "date": list(pd.to_datetime(filled_entry_orders["date"].iloc[:]).dt.date),
    "asset": "IVV",
    "trip": 'EXIT',
    "action": "SELL",
    "type": "LMT",
    "price": filled_entry_orders['price'].iloc[:] * (1 + alpha2),
    'status': 'SUBMITTED'
})
#print(submitted_exit_orders)
submitted_exit_orders = submitted_exit_orders.sort_values(["date", 'trade_id'])

# Cancelled sell orders
high_prices = pd.DataFrame({
        'High_price': ivv_prc['High Price'].iloc[:][::-1].rolling(n2).max()[::-1],
        'date': list(pd.to_datetime(ivv_prc["Date"].iloc[:]).dt.date)
        })

with np.errstate(invalid='ignore'):
    cancelled_exit_orders = submitted_exit_orders[
        np.greater(
            submitted_exit_orders['price'].to_numpy(),
            pd.merge(submitted_exit_orders, high_prices, on='date')['High_price'].to_numpy()
        )
    ].copy()
cancelled_exit_orders.reset_index(drop=True, inplace=True)
cancelled_exit_orders['status'] = 'CANCELLED'

cancelled_exit_orders['date'] = pd.DataFrame(
    {'cancel_date': ivv_prc['Date'].iloc[(n2-1):].to_numpy()},
    index=ivv_prc['Date'].iloc[:(1-n2)].to_numpy()
).loc[cancelled_exit_orders['date']]['cancel_date'].to_list()

#print(cancelled_exit_orders)


# filled exit orders
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
        ivv_prc['Date'] == filled_exit_orders['date'].iloc[i]
    )[0]

    ivv_slice = ivv_prc.iloc[idx1:(idx1+n2)]['High Price']

    fill_inds = ivv_slice >= filled_exit_orders['price'].iloc[i]

    if (len(fill_inds) < n2) & (not any(fill_inds)):
        filled_exit_orders.at[i, 'status'] = 'LIVE'
    else:
        filled_exit_orders.at[i, 'date'] = ivv_prc['Date'].iloc[
            fill_inds.idxmax()
        ]

live_exit_orders = filled_exit_orders[
    filled_exit_orders['status'] == 'LIVE'
    ]

filled_exit_orders = filled_exit_orders[
    filled_exit_orders['status'] == 'FILLED'
    ]

#print(live_exit_orders)
#print(filled_exit_orders)

#print(cancelled_exit_orders)

# Market to exit

market_exit_orders = pd.DataFrame({
    "trade_id": list(cancelled_exit_orders['trade_id']),
    "date": list(pd.to_datetime(cancelled_exit_orders["date"].iloc[:]).dt.date),
    "asset": "IVV",
    "trip": 'EXIT',
    "action": "SELL",
    "type": "MKT",
    "price": list(cancelled_exit_orders['price']),
    'status': 'FILLED'
})

# market_exit_orders['price'] = ivv_prc.loc[ivv_prc['Date'].isin(market_exit_orders['date'])]['Close Price'].to_list()

for i in range(0, len(market_exit_orders)):

    idx1 = np.flatnonzero(
        ivv_prc['Date'] == market_exit_orders['date'].iloc[i]
    )[0]

    market_exit_orders['price'].iloc[i] = ivv_prc['Close Price'].iloc[idx1]

#print(market_exit_orders)

exit_orders = pd.concat(
    [
        submitted_exit_orders,
        cancelled_exit_orders,
        filled_exit_orders,
        live_exit_orders,
        market_exit_orders
    ]
).sort_values(["date", 'trade_id'])

submitted_orders = pd.concat([submitted_entry_orders, submitted_exit_orders]).sort_values(['date']).reset_index(drop=True)
submitted_orders['trade_id'] = submitted_orders.index + 1

entry_trades = submitted_orders[submitted_orders["trip"] == "ENTER"]["trade_id"]
print(entry_trades)

mapping_entry = {k: v for k, v in zip(entry_orders["trade_id"].unique(), entry_trades)}
entry_orders = entry_orders.reset_index(drop=True)
entry_orders['trade_id'] = entry_orders['trade_id'].map(mapping_entry)

exit_trades = submitted_orders[submitted_orders["trip"] == "EXIT"]["trade_id"]
print(exit_trades)

mapping_exit = {k: v for k, v in zip(exit_orders["trade_id"].unique(), exit_trades)}
exit_orders = exit_orders.reset_index(drop=True)
exit_orders['trade_id'] = exit_orders['trade_id'].map(mapping_exit)

result = pd.concat([entry_orders, exit_orders]).sort_values(['date', 'trade_id'])

print("submitted_entry_orders:")
print(submitted_entry_orders)
print(" ")

print("cancelled_entry_orders:")
print(cancelled_entry_orders)
print(" ")

print("filled_entry_orders:")
print(filled_entry_orders)
print(" ")

print("live_entry_orders:")
print(live_entry_orders)
print(" ")

print("entry_orders:")
print(entry_orders)
print(" ")

print("submitted_exit_orders:")
print(submitted_exit_orders)
print(" ")

print("cancelled_exit_orders:")
print(cancelled_exit_orders)
print(" ")

print("filled_exit_orders:")
print(filled_exit_orders)
print(" ")

print("live_exit_orders:")
print(live_exit_orders)
print(" ")

print("market_exit_orders:")
print(market_exit_orders)
print(" ")

print("exit_orders:")
print(exit_orders)
print(" ")

print("submitted_orders:")
print(submitted_orders)
print(" ")

print("result:")
print(result)
print(" ")

__all__ = ['entry_orders', 'exit_orders']


