import json
import time
import logging
from datetime import datetime
from kafka import KafkaProducer
import pandas as pd
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATA_DIR = Path("data/raw")
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "olist-events"

def get_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )


def load_events() -> pd.DataFrame:
    orders = pd.read_csv(DATA_DIR / "olist_orders_dataset.csv")
    items = pd.read_csv(DATA_DIR / "olist_order_items_dataset.csv")
    customers = pd.read_csv(DATA_DIR / "olist_customers_dataset.csv")
    products = pd.read_csv(DATA_DIR / "olist_products_dataset.csv")

    # Enrich
    items_enriched = items.merge(
        products[["product_id", "product_category_name"]],
        on="product_id", how="left"
    )
    items_agg = (
        items_enriched.groupby("order_id")
        .agg(
            order_value=("price", "sum"),
            freight_value=("freight_value", "sum"),
            item_count=("order_item_id", "count"),
            product_category=("product_category_name", "first"),
        )
        .reset_index()
    )

    enriched = orders.merge(
        customers[["customer_id", "customer_state", "customer_city"]],
        on="customer_id", how="left"
    )
    enriched = enriched.merge(items_agg, on="order_id", how="left")

    event_map = {
        "order_purchase_timestamp": "order_placed",
        "order_approved_at": "order_approved",
        "order_delivered_carrier_date": "order_shipped",
        "order_delivered_customer_date": "order_delivered",
    }

    all_events = []
    for ts_col, event_type in event_map.items():
        subset = enriched[enriched[ts_col].notna()].copy()
        subset["event_type"] = event_type
        subset["occurred_at"] = subset[ts_col]
        all_events.append(subset[["order_id", "customer_id", "customer_state",
                                   "customer_city", "order_status", "order_value",
                                   "freight_value", "item_count", "product_category",
                                   "event_type", "occurred_at"]])

    events_df = pd.concat(all_events)
    events_df["occurred_at"] = pd.to_datetime(events_df["occurred_at"])

    events_df = events_df.sort_values("occurred_at").reset_index(drop=True)
    logger.info(f"Loaded {len(events_df)} events, sorted by timestamp")
    return events_df


def produce(events_df: pd.DataFrame):
    producer = get_producer()
    total = len(events_df)

    logger.info("Starting stream simulation (full speed)...")

    for i, row in events_df.iterrows():
        message = {
            "event_id": f"stream-{row['order_id']}-{row['event_type']}",
            "event_type": row["event_type"],
            "occurred_at": str(row["occurred_at"]),
            "ingested_at": str(datetime.utcnow()),
            "source": "stream",
            "payload": {
                "order_id": row["order_id"],
                "customer_id": row["customer_id"],
                "customer_state": row.get("customer_state"),
                "customer_city": row.get("customer_city"),
                "order_status": row.get("order_status"),
                "order_value": round(float(row["order_value"]), 2) if pd.notna(row.get("order_value")) else None,
                "freight_value": round(float(row["freight_value"]), 2) if pd.notna(row.get("freight_value")) else None,
                "item_count": int(row["item_count"]) if pd.notna(row.get("item_count")) else None,
                "product_category": row.get("product_category") if pd.notna(row.get("product_category")) else None,
            },
        }

        producer.send(KAFKA_TOPIC, key=row["order_id"], value=message)

        if (i + 1) % 10000 == 0:
            producer.flush()
            logger.info(f"Produced {i + 1}/{total} events")

    producer.flush()
    logger.info(f"Done! Produced {total} events")


if __name__ == "__main__":
    events_df = load_events()
    produce(events_df)