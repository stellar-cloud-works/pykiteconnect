import logging
from kiteconnect import KiteTicker
import json
import pandas as pd
import datetime
import os
import requests
from collections import deque

logging.basicConfig(level=logging.DEBUG)

# Use environment variables for credentials
API_KEY = os.getenv("KITE_API_KEY")
ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN")

# OHLC Service configuration
OHLC_SERVICE_URL = "http://localhost:5001"
USE_OHLC_SERVICE = True  # Set to False to disable OHLC processing

# Initialise
kws = KiteTicker(API_KEY, ACCESS_TOKEN)

# Local buffer for ticks (backup)
tick_buffer = deque(maxlen=1000)

def send_tick_to_ohlc_service(tick_data):
    """Send tick to OHLC microservice"""
    try:
        response = requests.post(
            f"{OHLC_SERVICE_URL}/add-tick",
            json=tick_data,
            timeout=1.0  # Quick timeout to avoid blocking
        )
        return response.status_code == 200
    except Exception as e:
        logging.warning(f"Failed to send tick to OHLC service: {e}")
        return False

def on_ticks(ws, ticks):  # noqa
    """Callback to receive ticks."""
    if len(ticks) > 0:
        # Write to JSONL file (existing functionality)
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
                
                # Write to file
                f.write(json.dumps(tick_copy) + '\n')
                
                # Add to local buffer
                tick_buffer.append(tick_copy)
                
                # Send to OHLC microservice if enabled
                if USE_OHLC_SERVICE:
                    send_tick_to_ohlc_service(tick_copy)
        
        logging.info("Ticks: {}".format(len(ticks)))

def on_connect(ws, response):  # noqa
    """Callback on successful connect."""
    logging.info("Connected to ticker. Response: {}".format(response))
    
    # Subscribe to a list of instrument_tokens
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

    # Set some instruments to tick in `full` mode.
    ws.set_mode(ws.MODE_FULL, [738561, 408065, 779521])

def on_order_update(ws, data):
    """Callback for order updates."""
    logging.debug("Order update : {}".format(data))

def on_error(ws, code, reason):
    """Callback for errors."""
    logging.error(f"Ticker error: {code} - {reason}")

def on_close(ws, code, reason):
    """Callback on connection close."""
    logging.info(f"Ticker connection closed: {code} - {reason}")

def get_latest_ohlc(instrument_token, interval=1):
    """Get latest OHLC data from microservice"""
    try:
        response = requests.get(
            f"{OHLC_SERVICE_URL}/ohlc/{instrument_token}/{interval}/latest",
            timeout=2.0
        )
        if response.status_code == 200:
            return response.json()
        return None
    except Exception as e:
        logging.warning(f"Failed to get OHLC data: {e}")
        return None

def export_ohlc_data(instrument_token, interval=1):
    """Export OHLC data in historical format"""
    try:
        response = requests.get(
            f"{OHLC_SERVICE_URL}/ohlc/{instrument_token}/{interval}/export",
            timeout=5.0
        )
        if response.status_code == 200:
            return response.json()
        return None
    except Exception as e:
        logging.warning(f"Failed to export OHLC data: {e}")
        return None

# Assign the callbacks.
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_order_update = on_order_update
kws.on_error = on_error
kws.on_close = on_close

if __name__ == "__main__":
    # Check if OHLC service is available
    if USE_OHLC_SERVICE:
        try:
            response = requests.get(f"{OHLC_SERVICE_URL}/health", timeout=2.0)
            if response.status_code == 200:
                logging.info("OHLC microservice is available")
            else:
                logging.warning("OHLC microservice health check failed")
                USE_OHLC_SERVICE = False
        except Exception as e:
            logging.warning(f"OHLC microservice not available: {e}")
            USE_OHLC_SERVICE = False
    
    logging.info(f"Starting ticker with OHLC service: {USE_OHLC_SERVICE}")
    
    # Infinite loop on the main thread. Nothing after this will run.
    # You have to use the pre-defined callbacks to manage subscriptions.
    try:
        kws.connect()
    except KeyboardInterrupt:
        logging.info("Ticker stopped by user")
    except Exception as e:
        logging.error(f"Ticker error: {e}")
