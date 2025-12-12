from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "bonus_22B030147",
    bootstrap_servers = ['localhost:9092'],
    auto_offset_reset = 'latest',
    value_deserializer = lambda v: json.loads(v.decode('utf-8'))
)

try:
    for message in consumer:
        print(f"Received :{message.value}")
except KeyboardInterrupt:
    consumer.close()
