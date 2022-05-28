import os
import json
import argparse

from pycon_kafka.helpers.kafka_helper import KafkaProducer
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
    
    corresponding_avro_serializer = schema_registry_client.get_serializer(
        schema_str=EXAMPLE_SCHEMA_STR
    )
    
    producer = KafkaProducer(
        kafka_brokers= args.kafka_brokers,
        avro_serializer=corresponding_avro_serializer
    )
    
    with open('./example_data/example.json') as f:
        example_dataset = json.load(f)
    
    producer.send_message(
        topic=EXAMPLE_TOPIC, 
        example_dataset=example_dataset
    )
    
        
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
    
    main(parser.parse_args())
    
