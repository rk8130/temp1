from decimal import *
from time import sleep
from uuid import uuid4, UUID
import time

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import pandas as pd
import numpy as np


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
        return
    print(f"Record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'THLKPUUTCUHXIKZN',
    'sasl.password': 'DR2WbhlALLjuFvF355rSw8fxNjqNluqPV0l3pVMHBKv6xqFTlJ2agBRMs+nRZksn'
}

# Schema Registry configuration
schema_registry_client = SchemaRegistryClient({
    'url': 'https://psrc-lo3do.us-east-2.aws.confluent.cloud',
    'basic.auth.user.info': '6NIZUTBC6SJCWNNB:LlZZDvA7BvgbBYGFoWbSpyQHlpXjHG2R89o81ahdZ4es6JooNPkjkMc7VALMY15I'
})

# Fetch the latest Avro schema
subject_name = 'topic_1-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Serializers
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,
    'value.serializer': avro_serializer
})

# Load and clean data
df = pd.read_excel('output.xlsx')

df = df.drop(['Unnamed: 0','description','images' , 'url'], axis = 1)

# Cleaning and typecasting
df['actual_price'] = df['actual_price'].str.replace(',', '', regex=True).fillna(0).astype(float)
df['selling_price'] = df['selling_price'].str.replace(',', '', regex=True).fillna(0).astype(float)

# Iterate over rows and produce to Kafka
for index, row in df.iterrows():
    key = str(row['_id'])
    value = row.to_dict()
    
    try:
        producer.produce(
            topic='topic_1',
            key=key,
            value=value,
            on_delivery=delivery_report
        )
        producer.flush()  # Ensures delivery

        # print(key,"  ",value)
        time.sleep(2)  # Adjust or remove as needed
    except Exception as e:
        print(f"Failed to produce record {key}: {e}")

print("All Data successfully published to Kafka.")
