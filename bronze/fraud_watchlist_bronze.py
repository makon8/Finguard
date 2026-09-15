from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col
import json

@dp.table(
    name="finguard2.bronze.fraud_watchlist",
    comment="Fraud watchlist raw stream data ingested by autoloader"
)
def fraud_watchlist_bronze() -> DataFrame:

    streaming_df = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.inferColumnTypes", "true")
              .load("/Volumes/finguard2/source/fraud_watchlist/source_data/")
    )

    parsed_streaming_df = streaming_df.select(
    F.col("watchlist_id"),
    F.col("watch_type"),
    F.col("status"),
    F.col("risk_level"),
    F.col("action"),
    F.col("reason_code"),
    F.col("reason_description"),
    F.col("reported_by"),
    F.col("country"),
    F.col("city"),
    F.col("effective_from"),
    F.col("_rescued_data"),
    F.col("_metadata.file_path").alias("source_file"),
    F.current_timestamp().alias("ingestion_timestamp")
    )
    return parsed_streaming_df