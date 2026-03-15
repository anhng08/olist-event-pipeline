from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Dict
import logging 

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATA_DIR = "/opt/spark/data/raw"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("olist-batch-extract")
        .master("spark://spark-master:7077")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "1")
        .getOrCreate()
    )


def load_raw_tables(spark: SparkSession) -> Dict[str, DataFrame]:
    files = {
        "orders": "olist_orders_dataset.csv",
        "order_items": "olist_order_items_dataset.csv",
        "customers": "olist_customers_dataset.csv",
        "reviews": "olist_order_reviews_dataset.csv",
        "products": "olist_products_dataset.csv",
    }

    tables = {}
    for name, filename in files.items():
        df = spark.read.csv(f"{DATA_DIR}/{filename}", header=True, inferSchema=True)
        tables[name] = df
        logger.info(f"Loaded {name}: {df.count()} rows")

    return tables


def build_enriched_orders(tables: Dict[str, DataFrame]) -> DataFrame:
    orders = tables["orders"]
    items = tables["order_items"]
    customers = tables["customers"]
    products = tables["products"]

    items_enriched = items.join(
        products.select("product_id", "product_category_name"),
        on="product_id",
        how="left",
    )

    items_array = items_enriched.groupBy("order_id").agg(
        F.collect_list(
            F.struct(
                F.col("product_id"),
                F.col("product_category_name").alias("category"),
                F.col("price"),
                F.col("freight_value"),
                F.col("order_item_id"),
            )
        ).alias("items"),
        F.round(F.sum("price"), 2).alias("order_value"),
        F.round(F.sum("freight_value"), 2).alias("freight_value"),
        F.count("order_item_id").alias("item_count"),
    )

    # Join customer geo
    enriched = orders.join(
        customers.select("customer_id", "customer_state", "customer_city"),
        on="customer_id",
        how="left",
    )

    # Join items array
    enriched = enriched.join(items_array, on="order_id", how="left")

    logger.info(f"Enriched orders: {enriched.count()} rows")
    return enriched


if __name__ == "__main__":
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    tables = load_raw_tables(spark)
    enriched = build_enriched_orders(tables)
    enriched.printSchema()
    enriched.show(3, truncate=True)
    spark.stop()