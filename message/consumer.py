import json
from datetime import datetime

import kafka

from repo import store_job_info
from message import get_kafka_url


class Consumer:
    server_url: str
    topic: str
    group_id: str
    time_out_sec: int
    _consumer: kafka.KafkaConsumer

    def __init__(self, topic: str = 'job_posts', group_id: str = 'data_harvester', time_out_sec: int = 10):
        self.server_url = get_kafka_url()
        self.topic = topic + '_' + datetime.now().strftime('%y%m%d')
        self.group_id = group_id
        self.time_out_sec = time_out_sec
        self._consumer = kafka.KafkaConsumer(
            self.topic,
            bootstrap_servers=self.server_url,
            group_id=self.group_id,
            enable_auto_commit=False,
            auto_offset_reset='earliest',
        )

    def consume_data(self):
        try:
            print(f'Consuming data from topic({self.topic}) as group({self.group_id})')
            messages = self._consumer.poll(timeout_ms=self.time_out_sec * 1000)
            if messages:
                for tp, msgs in messages.items():
                    for message in msgs:
                        received_data = message.value
                        print(f"Received message: {received_data=}")
                        decoded_data = received_data.decode('utf-8')

                        job_info = json.loads(decoded_data)
                        store_job_info(job_info)
            else:
                print('There is no new messages')
        except KeyboardInterrupt:
            print('Interrupted, closing consumer...')
        finally:
            self.close()

    def close(self):
        self._consumer.close()
        print(f'Close consumer')
