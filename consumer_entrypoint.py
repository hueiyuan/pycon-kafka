import os
import argparse

from pycon_kafka.helpers.kafka_helper import KafkaConsumer
from pycon_kafka.helpers.schema_registry_helper import SchemaRegistry

from dotenv import load_dotenv
load_dotenv()

SR_USR = os.getenv('SCHEMA_REGISTRY_USERNAME')
SR_PWD = os.getenv('SCHEMA_REGISTRY_PASSWORD')

EXAMPLE_TOPIC = 'test-topic'
EXAMPLE_SCHEMA_STR = """
    {
        "namespace": "schema_registry.examples.serialization.avro",
        "name": "User",
        "type": "record",
        "fields": [
            {
                "name": "name", 
                "type": "string"
            },
            {
                "name": "sex", 
                "type": "string"
            },
            {
                "name": "phone_number", 
                "type": "string"
            },
            {
                "name": "interest", 
                "type": [
                    "string",
                    "null"
                ],
			    "default": "null"
            }
        ]
    }
"""
    
def main(args):
    schema_registry_client = SchemaRegistry(
        schema_registry_endpoint=args.schema_registry_endpoint,
        auth_info=f'{SR_USR}:{SR_PWD}'
    )
    
    corresponding_avro_deserializer = schema_registry_client.get_deserializer(
        schema_str=EXAMPLE_SCHEMA_STR
    )
    
    consumer = KafkaConsumer(
        kafka_brokers=args.kafka_brokers,
        group_id=args.consumer_group_id,
        avro_deserializer=corresponding_avro_deserializer
    )
    
    consumer.consume_messages(EXAMPLE_TOPIC)
    
        
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Kafka Consumer with schema registy example")
    parser.add_argument('--schema-registry-endpoint', 
                        action="store",
                        type=str,
                        required=True,
                        help="schema registry endpoint")
    
    parser.add_argument('--kafka-brokers', 
                        action="store",
                        type=str,
                        required=True,
                        help="kafka brokers endpoint")
    
    parser.add_argument('--consumer-group-id', 
                        action="store",
                        type=str,
                        required=True,
                        help="consumer group id")
    
    main(parser.parse_args())
    
