###############################################################################
#
# The MIT License (MIT)
#
# Copyright (c) Zerodha Technology Pvt. Ltd.
#
# This example shows how to subscribe and get ticks from Kite Connect ticker,
# For more info read documentation - https://kite.trade/docs/connect/v1/#streaming-websocket
###############################################################################

import logging
from kiteconnect import KiteTicker
import json
import pandas as pd
import datetime

logging.basicConfig(level=logging.DEBUG)

# Use environment variables for credentials
API_KEY = os.getenv("KITE_API_KEY")
ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN")

# Initialise
kws = KiteTicker(API_KEY, ACCESS_TOKEN)

def on_ticks(ws, ticks):  # noqa
    # Callback to receive ticks.
    if len(ticks) > 0:
        with open('tick_data.jsonl', 'a') as f:
            for tick in ticks:
                # Create a copy of the tick to avoid modifying the original
                tick_copy = tick.copy()
                
                # Add a timestamp to each tick
                tick_copy['timestamp'] = pd.Timestamp.now().isoformat()
                
                # Convert any datetime objects to ISO format strings
                for key, value in tick_copy.items():
                    if isinstance(value, (datetime.datetime, pd.Timestamp)):
                        tick_copy[key] = value.isoformat()
                    elif hasattr(value, 'isoformat'):  # Handle other datetime-like objects
                        tick_copy[key] = value.isoformat()
                
                f.write(json.dumps(tick_copy) + '\n')
        logging.info("Ticks: {}".format(ticks))

def on_connect(ws, response):  # noqa
    # Callback on successful connect.
    # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
    ws.subscribe([
    408065, 738561, 341249, 1270529, 779521, 492033, 1510401, 1346049, 3050241,
    579329, 2863105, 261889, 81153, 119553, 5582849, 4774913, 6054401, 175361,
    4268801, 579329, 2953217, 408065, 969473, 1850625, 3465729, 1152769,
    4701441, 4752385, 356865, 424961, 4598529, 140033, 197633, 2585345, 1041153,
    3876097, 878593, 4843777, 857857, 225537, 177665, 2800641, 900609, 303617,
    70401, 418049, 2911489, 40193, 2815745, 884737, 519937, 232961, 2170625,
    345089, 54273, 108033, 558337, 738561, 633601, 415745, 134657, 1207553,
    4464129, 2905857, 2977281, 3834113, 6401, 895745, 3001089, 348929, 3924993,
    758529, 5215745, 758529, 1723649, 2939649, 112129, 951809, 2513665, 108033,
    806401, 3329, 3848705, 486657, 470529, 2714625, 3677697, 738561, 3431425,
    975873, 952577, 3721473
  ])

    # Set RELIANCE to tick in `full` mode.
    ws.set_mode(ws.MODE_FULL, [738561])

def on_order_update(ws, data):
    logging.debug("Order update : {}".format(data))

# Assign the callbacks.
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_order_update = on_order_update

# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
kws.connect()
