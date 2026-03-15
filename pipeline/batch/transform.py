from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType
from functools import reduce
from typing import Dict
import uuid
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

EVENT_TIMESTAMP_MAP = {
    "order_purchase_timestamp": "order_placed",
    "order_approved_at": "order_approved",
    "order_delivered_carrier_date": "order_shipped",
    "order_delivered_customer_date": "order_delivered",
}

uuid_udf = F.udf(lambda: str(uuid.uuid4()), StringType())


def explode_events(enriched: DataFrame) -> DataFrame:
    all_events = []

    for ts_col, event_type in EVENT_TIMESTAMP_MAP.items():
        subset = (
            enriched
            .filter(F.col(ts_col).isNotNull())
            .select(
                uuid_udf().alias("event_id"),
                F.lit(event_type).alias("event_type"),
                F.col(ts_col).cast(TimestampType()).alias("occurred_at"),
                F.current_timestamp().alias("ingested_at"),
                F.lit("batch").alias("source"),
                F.to_json(F.struct(
                    F.col("order_id"),
                    F.col("customer_id"),
                    F.col("customer_state"),
                    F.col("customer_city"),
                    F.col("order_status"),
                    F.round(F.col("order_value").cast("double"), 2).alias("order_value"),
                    F.round(F.col("freight_value").cast("double"), 2).alias("freight_value"),
                    F.col("item_count"),
                    F.col("items"),
                )).alias("payload"),
            )
        )
        all_events.append(subset)
        logger.info(f"{event_type}: {subset.count()} events")

    return reduce(lambda a, b: a.unionAll(b), all_events)


def add_review_events(reviews: DataFrame, enriched: DataFrame) -> DataFrame:
    reviews_enriched = reviews.join(
        enriched.select("order_id", "customer_id", "customer_state"),
        on="order_id",
        how="left",
    )

    return (
        reviews_enriched
        .filter(F.col("review_creation_date").isNotNull())
        .select(
            uuid_udf().alias("event_id"),
            F.lit("review_submitted").alias("event_type"),
            F.col("review_creation_date").cast(TimestampType()).alias("occurred_at"),
            F.current_timestamp().alias("ingested_at"),
            F.lit("batch").alias("source"),
            # ✅ Cùng kiểu string với order events
            F.to_json(F.struct(
                F.col("order_id"),
                F.col("customer_id"),
                F.col("customer_state"),
                F.col("review_score").cast("int"),
                F.col("review_id"),
            )).alias("payload"),
        )
    )


if __name__ == "__main__":
    from extract import get_spark, load_raw_tables, build_enriched_orders

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    tables = load_raw_tables(spark)
    enriched = build_enriched_orders(tables)

    order_events = explode_events(enriched)
    review_events = add_review_events(tables["reviews"], enriched)

    all_events = order_events.unionAll(review_events)

    total = all_events.count()
    logger.info(f"Grand total events: {total}")
    all_events.printSchema()
    all_events.show(3, truncate=80)

    spark.stop()