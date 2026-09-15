from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

@dp.table(
    name="finguard2.gold.fraud_card_alert",
    comment="Alert details with transaction has been performed with value greater than what is set by customer"
)
def fraud_card_alert() -> DataFrame:
    transactions = spark.readStream.table("finguard2.silver.transactions")
    fraud_watchlist = spark.readStream.table("finguard2.silver.fraud_watchlist")

    customers = spark.read.table("finguard2.silver.customers")

    # Convert transaction_timestamp from string to timestamp for watermarking
    transactions = transactions.withColumn(
        "transaction_timestamp", 
        F.to_timestamp(F.col("transaction_timestamp"))
    )

    transactions_with_watermark = transactions.withWatermark("transaction_timestamp", "5 minutes")
    fraud_watchlist_with_watermark = fraud_watchlist.withWatermark("effective_from", "5 minutes")

    fraud_detected = (
        transactions_with_watermark.join(
            fraud_watchlist_with_watermark,
            transactions_with_watermark.card_number == fraud_watchlist_with_watermark.watchlist_id, "inner"
        )
        .join(
            customers,
            transactions_with_watermark.customer_id == customers.customer_id 
        )
        .select(
            #alert_identification
            F.concat_ws("-", F.lit("FRAUD"), F.col("transaction_id"), F.col("watchlist_id")).alias("alert_id"),
            F.lit("FRAUD_WATCHLIST_MATCH").alias("alert_type"),
            F.current_timestamp().alias("alert_timestamp"),

            #transaction_details
            transactions_with_watermark.transaction_id,
            transactions_with_watermark.customer_id,
            customers.email.alias("customer_email"),
            F.concat_ws(" ", customers.first_name, customers.last_name).alias("customer_name"),
            transactions_with_watermark.card_number,
            transactions_with_watermark.amount,
            transactions_with_watermark.currency,
            transactions_with_watermark.merchant_id,
            transactions_with_watermark.merchant_name,
            transactions_with_watermark.merchant_category,
            transactions_with_watermark.transaction_type,
            transactions_with_watermark.payment_channel,
            transactions_with_watermark.device_id,
            transactions_with_watermark.city.alias("transaction_city"),
            transactions_with_watermark.country.alias("transaction_country"),
            transactions_with_watermark.transaction_timestamp,

            #fraud_watchlist_details
            F.col("watchlist_id"),
            F.col("watch_type"),
            F.col("risk_level"),
            F.col("action"),
            F.col("reason_code"),
            F.col("reason_description"),
            F.col("effective_from").alias("watchlist_effective_from"),
            F.col("reported_by"),
            fraud_watchlist_with_watermark.city.alias("watchlist_city"),
            fraud_watchlist_with_watermark.country.alias("watchlist_country")
        )
    )
    
    return fraud_detected



