from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {
        'sensor_id': random.randint(1, 5),
        'temperature': round(random.uniform(20.0, 30.0), 2),
        'humidity': round(random.uniform(40.0, 80.0), 2),
        'timestamp': datetime.now().isoformat()
    }
    producer.send('sensor_data', value=data)
    print(f"Enviado: {data}")
    time.sleep(2)
