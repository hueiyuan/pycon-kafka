from dataclasses import dataclass

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.avro import AvroDeserializer

@dataclass
class SchemaRegistry:
    schema_registry_endpoint: str
    auth_info: str
    
    def __post_init__(self):
        schema_config = {
            'url': self.schema_registry_endpoint,
            'basic.auth.user.info': self.auth_info
        }
        
        self.sr_client = SchemaRegistryClient(schema_config)

    def get_serializer(self, schema_str: str) -> AvroSerializer:
        return AvroSerializer(
            schema_registry_client=self.sr_client,
            schema_str=schema_str
        )
        
    def get_deserializer(self, schema_str: str) -> AvroDeserializer:
        return AvroDeserializer(
            schema_registry_client=self.sr_client,
            schema_str=schema_str
        )
        
