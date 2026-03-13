from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'raw-telemetry',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("AI Service is listening...")
for message in consumer:
    event = message.value
    print(f"AI Service received: {event}")
    # כאן בהמשך סטודנט ב' יכניס את המודל שלו: model.predict(event)