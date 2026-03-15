import logging
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def validate_events(df: DataFrame) -> DataFrame:
    total = df.count()

    # 1. Drop null critical fields
    df_clean = df.filter(
        F.col("payload.order_id").isNotNull() &
        F.col("payload.customer_id").isNotNull() &
        F.col("occurred_at").isNotNull()
    )

    # 2. Drop negative order values
    df_clean = df_clean.filter(
        F.col("payload.order_value").isNull() |
        (F.col("payload.order_value") >= 0)
    )

    # 3. Drop future timestamps (data error)
    df_clean = df_clean.filter(
        F.col("occurred_at") <= F.current_timestamp()
    )

    clean_count = df_clean.count()
    dropped = total - clean_count
    logger.info(f"Validation: {clean_count}/{total} passed, {dropped} dropped ({dropped/total*100:.1f}%)")

    return df_clean


def validate_reviews(df: DataFrame) -> DataFrame:
    total = df.count()

    df_clean = df.filter(
        F.col("payload.order_id").isNotNull() &
        F.col("payload.review_score").isNotNull() &
        F.col("payload.review_score").between(1, 5)
    )

    clean_count = df_clean.count()
    logger.info(f"Reviews validation: {clean_count}/{total} passed")
    return df_clean