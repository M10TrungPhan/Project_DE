from typing import Dict, List
import json
from kafka import KafkaConsumer, TopicPartition

from ride import Ride
from settings import *


class JsonConsumer:
    def __init__(self, props: Dict) -> None:
        self.consumer = KafkaConsumer(**props)

    def consume_from_kafka(self, topics: List[str]):
        self.consumer.subscribe(topics)
        print('Consuming from kafka started')
        print(f'Topics comsume: {self.consumer.subscription()}')

        while True:
            try:
                message = self.consumer.poll(1.0)
                if message is None or message == {}:
                    continue
                print(message.keys())
                print("__________"*5)
                for message_key, message_value in message.items():
                    print(message_key)
                    print(message_value)
                    for msg_val in message_value:
                        print(msg_val.key, msg_val.value)
                        print("__")
            except KeyboardInterrupt:
                break
        self.consumer.close()
    
if __name__ == '__main__':
    config = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'auto_offset_reset': 'Lastest',
        'enable_auto_commit': True,
        'key_deserializer': lambda key: int(key.decode('utf-8')),
        'value_deserializer': lambda x: json.loads(x.decode('utf-8'), object_hook=lambda d: Ride.from_dict(d)),
        'group_id': 'consumer.group.id.json-example.1'
    }

    json_consumer = JsonConsumer(props=config)
    json_consumer.consume_from_kafka(topics=[KAFKA_TOPIC])