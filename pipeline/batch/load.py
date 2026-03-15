from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from typing import Dict
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError
import json
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0")
MONGO_DB = os.getenv("MONGO_DB", "olist")
BATCH_SIZE = 1000


def setup_indexes():
    client = MongoClient(MONGO_URI)
    col = client[MONGO_DB]["events"]

    col.create_index([("event_type", ASCENDING)])
    col.create_index([("occurred_at", DESCENDING)])
    col.create_index([("payload.customer_state", ASCENDING)])
    col.create_index([("payload.order_id", ASCENDING)])
    col.create_index([("source", ASCENDING)])
    col.create_index([("event_type", ASCENDING), ("occurred_at", DESCENDING)])

    logger.info("Indexes created")
    client.close()


def insert_partition(rows):
    from pymongo import MongoClient
    from pymongo.errors import BulkWriteError
    import json

    mongo_uri = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"
    mongo_db = "olist"

    client = MongoClient(mongo_uri)
    col = client[mongo_db]["events"]

    batch = []
    inserted = 0

    for row in rows:
        doc = {
            "event_id": row["event_id"],
            "event_type": row["event_type"],
            "occurred_at": row["occurred_at"],
            "ingested_at": row["ingested_at"],
            "source": row["source"],
            "payload": json.loads(row["payload"]),
        }
        batch.append(doc)

        if len(batch) >= BATCH_SIZE:
            try:
                col.insert_many(batch, ordered=False)
                inserted += len(batch)
            except BulkWriteError as e:
                inserted += e.details.get("nInserted", 0)
            batch = []

    if batch:
        try:
            col.insert_many(batch, ordered=False)
            inserted += len(batch)
        except BulkWriteError as e:
            inserted += e.details.get("nInserted", 0)

    client.close()
    yield inserted


def load_to_mongodb(all_events: DataFrame):
    logger.info("Loading events into MongoDB...")

    all_events.foreachPartition(insert_partition)

    client = MongoClient(MONGO_URI)
    total = client[MONGO_DB]["events"].count_documents({})
    event_types = client[MONGO_DB]["events"].distinct("event_type")
    logger.info(f"Total documents in MongoDB: {total}")
    logger.info(f"Event types: {event_types}")
    client.close()


if __name__ == "__main__":
    from extract import get_spark, load_raw_tables, build_enriched_orders
    from transform import explode_events, add_review_events

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    tables = load_raw_tables(spark)
    enriched = build_enriched_orders(tables)

    order_events = explode_events(enriched)
    review_events = add_review_events(tables["reviews"], enriched)
    all_events = order_events.unionAll(review_events)

    setup_indexes()
    load_to_mongodb(all_events)

    spark.stop()