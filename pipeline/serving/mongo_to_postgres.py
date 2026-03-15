import logging
from datetime import datetime
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_batch

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

MONGO_URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"
MONGO_DB = "olist"

PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "olist_analytics",
    "user": "olist",
    "password": "olist",
}


def get_mongo_col(name: str):
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB][name]


def get_pg_conn():
    return psycopg2.connect(**PG_CONFIG)


def setup_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fct_events (
                event_id        TEXT PRIMARY KEY,
                event_type      TEXT,
                occurred_at     TIMESTAMP,
                ingested_at     TIMESTAMP,
                source          TEXT,
                order_id        TEXT,
                customer_id     TEXT,
                customer_state  TEXT,
                customer_city   TEXT,
                order_status    TEXT,
                order_value     FLOAT,
                freight_value   FLOAT,
                item_count      INT,
                product_category TEXT
            );

            CREATE TABLE IF NOT EXISTS fct_reviews (
                event_id        TEXT PRIMARY KEY,
                occurred_at     TIMESTAMP,
                ingested_at     TIMESTAMP,
                order_id        TEXT,
                customer_id     TEXT,
                customer_state  TEXT,
                review_score    INT
            );

            -- ✅ Bảng mới — flatten items array từ MongoDB
            CREATE TABLE IF NOT EXISTS fct_order_items (
                order_id            TEXT,
                product_id          TEXT,
                category            TEXT,
                price               FLOAT,
                freight_value       FLOAT,
                order_item_id       INT,
                customer_state      TEXT,
                occurred_at         TIMESTAMP,
                PRIMARY KEY (order_id, product_id, order_item_id)
            );
        """)
        conn.commit()
        logger.info("Tables created")


def sync_order_items(conn):
    """Flatten items array từ MongoDB events → PostgreSQL rows"""
    col = get_mongo_col("events")

    # Chỉ lấy order_placed để tránh duplicate
    cursor = col.find({"event_type": "order_placed"})

    rows = []
    skipped = 0

    for doc in cursor:
        p = doc.get("payload", {})
        items = p.get("items", [])

        if not items:
            skipped += 1
            continue

        for item in items:
            product_id = item.get("product_id")
            if not product_id:
                continue

            rows.append((
                p.get("order_id"),
                product_id,
                item.get("category"),
                float(item.get("price")) if item.get("price") is not None else None,
                float(item.get("freight_value")) if item.get("freight_value") is not None else None,
                int(item.get("order_item_id")) if item.get("order_item_id") is not None else None,
                p.get("customer_state"),
                doc.get("occurred_at"),
            ))

    logger.info(f"Order items: {len(rows)} rows, {skipped} orders skipped")

    with conn.cursor() as cur:
        execute_batch(cur, """
            INSERT INTO fct_order_items (
                order_id, product_id, category, price, freight_value,
                order_item_id, customer_state, occurred_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (order_id, product_id, order_item_id) DO NOTHING
        """, rows, page_size=1000)
        conn.commit()

    logger.info(f"Synced {len(rows)} order items to PostgreSQL")


def sync_order_events(conn):
    col = get_mongo_col("events")
    cursor = col.find({"event_type": {"$ne": "review_submitted"}})

    rows = []
    skipped = 0

    for doc in cursor:
        p = doc.get("payload", {})

        # Validate critical fields
        if not p.get("order_id") or not p.get("customer_id"):
            skipped += 1
            continue

        order_value = p.get("order_value")
        if order_value is not None and order_value < 0:
            skipped += 1
            continue

        rows.append((
            doc.get("event_id"),
            doc.get("event_type"),
            doc.get("occurred_at"),
            doc.get("ingested_at"),
            doc.get("source"),
            p.get("order_id"),
            p.get("customer_id"),
            p.get("customer_state"),
            p.get("customer_city"),
            p.get("order_status"),
            float(order_value) if order_value is not None else None,
            float(p.get("freight_value")) if p.get("freight_value") is not None else None,
            int(p.get("item_count")) if p.get("item_count") is not None else None,
            p.get("product_category"),
        ))

    logger.info(f"Order events: {len(rows)} valid, {skipped} skipped")

    with conn.cursor() as cur:
        execute_batch(cur, """
            INSERT INTO fct_events (
                event_id, event_type, occurred_at, ingested_at, source,
                order_id, customer_id, customer_state, customer_city,
                order_status, order_value, freight_value, item_count, product_category
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (event_id) DO NOTHING
        """, rows, page_size=1000)
        conn.commit()

    logger.info(f"Synced {len(rows)} order events to PostgreSQL")


def sync_review_events(conn):
    col = get_mongo_col("events")
    cursor = col.find({"event_type": "review_submitted"})

    rows = []
    skipped = 0

    for doc in cursor:
        p = doc.get("payload", {})

        # col4 là review_score do PySpark struct bug
        review_score = p.get("review_score") or p.get("col4")

        if not p.get("order_id") or review_score is None:
            skipped += 1
            continue

        if not (1 <= int(review_score) <= 5):
            skipped += 1
            continue

        rows.append((
            doc.get("event_id"),
            doc.get("occurred_at"),
            doc.get("ingested_at"),
            p.get("order_id"),
            p.get("customer_id"),
            p.get("customer_state"),
            int(review_score),
        ))

    logger.info(f"Reviews: {len(rows)} valid, {skipped} skipped")

    with conn.cursor() as cur:
        execute_batch(cur, """
            INSERT INTO fct_reviews (
                event_id, occurred_at, ingested_at,
                order_id, customer_id, customer_state, review_score
            ) VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (event_id) DO NOTHING
        """, rows, page_size=1000)
        conn.commit()

    logger.info(f"Synced {len(rows)} review events to PostgreSQL")


if __name__ == "__main__":
    logger.info("Starting MongoDB → PostgreSQL sync...")
    conn = get_pg_conn()
    setup_tables(conn)
    sync_order_events(conn)
    sync_review_events(conn)
    sync_order_items(conn)

    with conn.cursor() as cur:
        cur.execute("SELECT event_type, COUNT(*) FROM fct_events GROUP BY event_type ORDER BY count DESC")
        logger.info("fct_events breakdown:")
        for row in cur.fetchall():
            logger.info(f"  {row[0]}: {row[1]}")

        cur.execute("SELECT COUNT(*) FROM fct_reviews")
        logger.info(f"fct_reviews: {cur.fetchone()[0]} rows")

    conn.close()