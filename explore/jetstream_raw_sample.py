"""Capture raw Jetstream events (all collections) for analysis."""
import json
import sys
import websocket

url = "wss://jetstream2.us-east.bsky.network/subscribe"
count = 0
MAX = 500

def on_message(ws, message):
    global count
    print(message)
    count += 1
    if count >= MAX:
        ws.close()

def on_open(ws):
    print(f"Connected. Capturing {MAX} raw events...", file=sys.stderr)

ws = websocket.WebSocketApp(url, on_message=on_message, on_open=on_open)
ws.run_forever()
