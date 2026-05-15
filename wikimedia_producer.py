#!/usr/bin/env python3
import json
import requests
from kafka import KafkaProducer

KAFKA_BROKER = 'kafka-server:9092'
TOPIC = 'wikimedia-events'
URL = "https://stream.wikimedia.org/v2/stream/recentchange"
HEADERS = {
    "User-Agent": "cs523-final-project/1.0 (dwabuluka@gmail.com)",
    "Accept": "text/event-stream",
}

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def stream_events():
    print(f"Connecting to {URL} ...")
    with requests.get(URL, headers=HEADERS, stream=True) as resp:
        resp.raise_for_status()
        print("Connected. Streaming events...")
        for raw_line in resp.iter_lines():
            if not raw_line:
                continue
            line = raw_line.decode('utf-8')
            if not line.startswith('data:'):
                continue
            data_str = line[5:].strip()
            try:
                event = json.loads(data_str)
                producer.send(TOPIC, event)
                print(f"Sent: {event.get('title', 'unknown')}")
            except Exception as e:
                print(f"Error parsing event: {e}")

if __name__ == "__main__":
    stream_events()
