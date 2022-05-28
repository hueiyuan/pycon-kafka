import json
import certifi
from uuid import uuid4
from datetime import datetime
from dataclasses import dataclass
from typing import List, Dict

from confluent_kafka import KafkaException
## confluent kafka producer and avro serializer
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry.avro import AvroSerializer

## confluent kafka consumer and avro deserializer
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.avro import AvroDeserializer

@dataclass
class KafkaProducer:
    kafka_brokers: str
    avro_serializer: AvroSerializer
    
    def __post_init__(self):
        producer_config = {
            'bootstrap.servers': 'SSL://' + self.kafka_brokers,
            'security.protocol': 'SSL',
            'ssl.ca.location': certifi.where(),
            'batch.size': 1024 * 1024 * 2,
            'message.max.bytes': 1024 * 1024 * 2,
            'linger.ms': 10000,
            'sticky.partitioning.linger.ms': 20000,
            'compression.type': 'lz4',
            'error_cb': self.__error_callback_func,
            'value.serializer': self.avro_serializer
        }
    
        self.producer = SerializingProducer(producer_config)
    
    def __error_callback_func(self, kafka_error) -> KafkaException:
        raise KafkaException(kafka_error)

    def __delivery_func(self, err, msg):
        if err is not None:
            print('Message delivery failed: {}'.format(err))
            raise KafkaException(err)
        
        
        msg_callback_infomation = {
            'latency': msg.latency(),
            'kafka_offset': msg.offset(),
            'topic_partition': msg.partition(),
            'data_tag': msg.topic(),
            'produce_kafka_time': datetime.fromtimestamp(msg.timestamp()[1] / 1e3).strftime("%Y-%m-%d %H:%M:%S.%f"),
        }

        
    def send_message(self, 
                     topic: str, 
                     example_dataset: List[Dict]) -> None:
        
        for data in example_dataset:
            self.producer.poll(0)
            
            
            self.producer.produce(
                topic=topic,
                value=data,
                on_delivery=self.__delivery_func)

        print("\nFlushing records...")
        self.producer.flush()
        
        
        
@dataclass
class KafkaConsumer:
    kafka_brokers: str
    group_id: str
    avro_deserializer: AvroDeserializer
    
    def __post_init__(self):
        consumer_config = {
            'bootstrap.servers': 'SSL://' + self.kafka_brokers,
            'security.protocol': 'SSL',
            'ssl.ca.location': certifi.where(),
            'value.deserializer': self.avro_deserializer,
            'group.id': self.group_id,
            'error_cb': self.__error_callback_func,
            'auto.offset.reset': "earliest"
        }
    
        self.consumer = DeserializingConsumer(consumer_config)
        
    def __error_callback_func(self, kafka_error) -> KafkaException:
        raise KafkaException(kafka_error)
    
    def consume_messages(self, 
                        topic: str) -> None:
        
        self.consumer.subscribe([topic])
        
        while True:
            try:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue

                data_msg = msg.value()
                if data_msg is not None:
                    print(data_msg)
            except KeyboardInterrupt:
                break

        self.consumer.close()
        
        
