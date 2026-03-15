import json
import logging
from datetime import datetime
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "olist-events"
KAFKA_GROUP_ID = "olist-consumer-group"
MONGO_URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"
MONGO_DB = "olist"
BATCH_SIZE = 500


def get_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        max_poll_records=BATCH_SIZE,
        consumer_timeout_ms=10000,
    )

def get_collection():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB]["stream_events"]


def consume():
    consumer = get_consumer()
    col = get_collection()
    batch = []
    total_inserted = 0

    logger.info(f"Consumer started, listening on '{KAFKA_TOPIC}'...")

    for message in consumer:
        event = message.value
        event["kafka_offset"] = message.offset
        event["kafka_partition"] = message.partition
        batch.append(event)

        if len(batch) >= BATCH_SIZE:
            try:
                col.insert_many(batch, ordered=False)
                total_inserted += len(batch)
            except BulkWriteError as e:
                total_inserted += e.details.get("nInserted", 0)

            consumer.commit()  
            logger.info(f"Inserted batch — total so far: {total_inserted}")
            batch = []

    if batch:
        col.insert_many(batch, ordered=False)
        consumer.commit()
        total_inserted += len(batch)

    logger.info(f"Consumer finished. Total inserted: {total_inserted}")


if __name__ == "__main__":
    consume()