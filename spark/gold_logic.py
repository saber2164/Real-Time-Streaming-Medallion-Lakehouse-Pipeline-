"""Gold aggregation logic shared by the batch compaction job and the live refresher.

Keeping the transformation in one module means ``gold_compaction.py`` (the
end-of-run batch job) and ``gold_refresh.py`` (the dashboard's continuous
refresher) can never drift apart. Both write exactly the same Gold grain.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    count,
    date_trunc,
    sum as _sum,
    to_date,
)

from config import GOLD_GRAIN_COLUMNS


def build_gold_dataframe(silver: DataFrame) -> DataFrame:
    """Aggregate Silver to the Gold grain.

    Only ``order_created`` events count towards revenue/orders so that
    ``order_paid`` / ``order_cancelled`` rows never double count an order.
    """
    created = silver.filter(col("event_type") == "order_created")
    return (
        created.withColumn("hour_start", date_trunc("hour", col("event_time")))
        .groupBy(*GOLD_GRAIN_COLUMNS)
        .agg(
            count("*").alias("hourly_order_count"),
            _sum("price").alias("total_revenue"),
            _sum("freight_value").alias("total_freight"),
            avg("freight_value").alias("avg_freight_cost"),
        )
        .withColumn("event_date", to_date(col("hour_start")))
    )


def write_gold(gold_df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """Write the Gold aggregate to Delta.

    ``overwriteSchema`` is safe because Gold is a fully derived aggregate:
    every run recomputes the same schema from Silver.
    """
    (
        gold_df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .save(path)
    )
