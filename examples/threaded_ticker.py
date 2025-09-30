###############################################################################
#
# The MIT License (MIT)
#
# Copyright (c) Zerodha Technology Pvt. Ltd.
#
# This example shows how to run KiteTicker in threaded mode.
# KiteTicker runs in seprate thread and main thread is blocked to juggle between
# different modes for current subscribed tokens. In real world web apps
# the main thread will be your web server and you can access WebSocket object
# in your main thread while running KiteTicker in separate thread.
###############################################################################

import time
import logging
from kiteconnect import KiteTicker

logging.basicConfig(level=logging.DEBUG)

# Use environment variables for credentials
API_KEY = os.getenv("KITE_API_KEY")
ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN")

# Initialise
kws = KiteTicker(API_KEY, ACCESS_TOKEN)

# RELIANCE BSE
tokens = [
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
  ]


# Callback for tick reception.
def on_ticks(ws, ticks):
    if len(ticks) > 0:
        logging.info("Current mode: {}".format(ticks[0]["mode"]))


# Callback for successful connection.
def on_connect(ws, response):
    logging.info("Successfully connected. Response: {}".format(response))
    ws.subscribe(tokens)
    ws.set_mode(ws.MODE_FULL, tokens)
    logging.info("Subscribe to tokens in Full mode: {}".format(tokens))


# Callback when current connection is closed.
def on_close(ws, code, reason):
    logging.info("Connection closed: {code} - {reason}".format(code=code, reason=reason))


# Callback when connection closed with error.
def on_error(ws, code, reason):
    logging.info("Connection error: {code} - {reason}".format(code=code, reason=reason))


# Callback when reconnect is on progress
def on_reconnect(ws, attempts_count):
    logging.info("Reconnecting: {}".format(attempts_count))


# Callback when all reconnect failed (exhausted max retries)
def on_noreconnect(ws):
    logging.info("Reconnect failed.")


# Assign the callbacks.
kws.on_ticks = on_ticks
kws.on_close = on_close
kws.on_error = on_error
kws.on_connect = on_connect
kws.on_reconnect = on_reconnect
kws.on_noreconnect = on_noreconnect

# Infinite loop on the main thread.
# You have to use the pre-defined callbacks to manage subscriptions.
kws.connect(threaded=True)

# Block main thread
logging.info("This is main thread. Will change webosocket mode every 5 seconds.")

while True:
    if kws.is_connected():
        logging.info("### Set mode to quote for all tokens")
        kws.set_mode(kws.MODE_QUOTE, tokens)
    time.sleep(5)
