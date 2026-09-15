from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col
import json

@dp.table(
    name="finguard2.bronze.transactions",
    comment="Transactions raw stream data ingested by Kafka"
)
def transactions_bronze() -> DataFrame:
    # Get Kafka connection details from Databricks secrets
    kafka_connection_json = dbutils.secrets.get(scope="finguard-scope", key="kafka_connection_details")
    kafka_config = json.loads(kafka_connection_json)
    kafka_bootstrap_servers = kafka_config['bootstrap_servers']
    kafka_api_key = kafka_config['api_key']
    kafka_api_secret = kafka_config['api_secret']
    kafka_topic = kafka_config['topic']
    
    jaas_config = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_api_key}" password="{kafka_api_secret}";'

    streaming_df = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", kafka_topic)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", jaas_config)
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("startingOffsets", "earliest")
    .load()
    )

    parsed_streaming_df = streaming_df.select(
    col("key").cast("string"),
    col("value").cast("string"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("timestampType"),
    F.current_timestamp().alias("ingestion_timestamp")
    )
    return parsed_streaming_df