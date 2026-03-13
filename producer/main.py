from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def generate_event():
    users = ["admin", "user1", "guest", "system"]
    events = [4624, 4625, 4648] # קודים של לוגים של ווינדוס
    return {
        "event_id": random.choice(events),
        "user": random.choice(users),
        "timestamp": time.time(),
        "ip": f"192.168.1.{random.randint(1, 254)}"
    }

print("Starting Producer... Press Ctrl+C to stop.")
while True:
    event = generate_event()
    producer.send('raw-telemetry', value=event)
    print(f"Sent: {event}")
    time.sleep(2) # שולח אירוע כל 2 שניות