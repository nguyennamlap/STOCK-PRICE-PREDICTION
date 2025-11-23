from kafka import KafkaProducer
import json
import os

TOPIC = os.getenv("KAFKA_TOPIC", "stock_price")


BROKER = os.getenv("BROKER_LIST", "kafka:9092").split(",")

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    key_serializer=lambda v: v.encode("utf-8"),
    compression_type="gzip",
    linger_ms=100,
    batch_size=16384
)


def send_to_kafka(symbol: str, message: dict):
    producer.send(TOPIC, key=symbol, value=message)
    producer.flush()