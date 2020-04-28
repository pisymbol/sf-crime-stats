import logging
import json

from kafka import KafkaConsumer

if __name__ == '__main__':
    topic = "com.udacity.crime.sf.stats.v1"
    consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id='my-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    for msg in consumer:
        if msg:
            print(msg.value)
