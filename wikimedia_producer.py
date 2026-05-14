#!/usr/bin/env python
import json
import time
from kafka import KafkaProducer
import ssl
import websocket

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Wikimedia SSE stream URL (Server-Sent Events)
URL = "wss://stream.wikimedia.org/v2/stream/recentchange"

def on_message(ws, message):
    if message.startswith("data:"):
        try:
            # Strip 'data: ' prefix and parse JSON
            data_str = message[5:].strip()
            event = json.loads(data_str)
            # Send to Kafka topic
            producer.send('wikimedia-events', event)
            print(f"Sent: {event.get('title', 'unknown')}")
        except Exception as e:
            print(f"Error: {e}")

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed")

def on_open(ws):
    print("Connected to Wikimedia stream")

if __name__ == "__main__":
    websocket.enableTrace(False)
    ws = websocket.WebSocketApp(URL,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.run_forever()
