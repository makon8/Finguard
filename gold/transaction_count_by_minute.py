from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

@dp.table(
    name="finguard2.gold.transaction_count_by_minute",
    comment="Count of transactions aggregated by 1-minute time windows"
)
def transaction_count_by_minute() -> DataFrame:
    transactions = spark.readStream.table("finguard2.silver.transactions")

    # Convert transaction_timestamp from string to timestamp for watermarking
    transactions = transactions.withColumn(
        "transaction_timestamp",
        F.to_timestamp(F.col("transaction_timestamp"))
    )

    transactions_with_watermark = transactions.withWatermark("transaction_timestamp", "5 minutes")

    transaction_count_df = transactions_with_watermark.groupBy(
        F.window("transaction_timestamp", "1 minute")
    ).agg(
        F.count("*").alias("transaction_count")  
    ).select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("transaction_count")
    )
    return transaction_count_df
    