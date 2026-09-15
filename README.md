# FinGuard2 Streaming Pipeline

Real-time fraud detection and transaction monitoring pipeline built with **Lakeflow Spark Declarative Pipelines (SDP)** on Databricks. The pipeline ingests streaming transaction data from Kafka and fraud watchlist files from cloud storage, processes them through a medallion architecture (bronze → silver → gold), and generates real-time fraud alerts with email notifications.

## Architecture

```
Kafka (Transactions) ──► Bronze ──► Silver ──► Gold ──► Email Alerts
                        Layer       Layer       Layer     (SMTP/Gmail)
Auto Loader (Watchlist) ─► Bronze ──► Silver ──► Gold
UC Volume (JSON files)    Layer       Layer       Layer
                                              │
                                    ┌─────────┘
                                    ▼
                          AI/BI Dashboard (Lakeview)
```

## Pipeline Configuration

| Setting | Value |
| --- | --- |
| Catalog | `finguard2` |
| Compute | Serverless + Photon |
| Channel | Current |
| Language | Python (PySpark SDP) |

## Medallion Layers

### Bronze (Raw Ingestion)

| Dataset | Source | Description |
| --- | --- | --- |
| `finguard2.bronze.transactions` | Kafka (SASL_SSL) | Raw transaction stream from Confluent Kafka, including key, value, topic, partition, offset, and ingestion timestamp |
| `finguard2.bronze.fraud_watchlist` | Auto Loader (JSON) | Fraud watchlist entries loaded from UC Volume (`/Volumes/finguard2/source/fraud_watchlist/source_data/`) |

### Silver (Cleaned & Parsed)

| Dataset | Description | Data Quality |
| --- | --- | --- |
| `finguard2.silver.transactions` | Parsed JSON from Kafka with explicit schema (16 fields), retaining Kafka metadata | `expect_or_drop` on transaction_id, customer_id, card_number, merchant_id; `expect` on amount > 0 |
| `finguard2.silver.fraud_watchlist` | Cleaned watchlist data with normalized casing, parsed timestamps, and source file tracking | - |

> **Note:** `finguard2.silver.customers` is ingested by a separate pipeline (`finguard2_customers_silver_ingestion`).

### Gold (Analytics & Alerts)

| Dataset | Type | Description |
| --- | --- | --- |
| `finguard2.gold.transaction_count_by_minute` | Streaming Table | Tumbling 1-minute windowed count of transactions (5-min watermark) |
| `finguard2.gold.transaction_count_by_minute_sliding_window` | Streaming Table | Sliding 5-minute window with 1-minute slide interval (5-min watermark) |
| `finguard2.gold.fraud_card_alert` | Streaming Table | Fraud alerts from stream-stream join of transactions and fraud watchlist (card_number = watchlist_id), enriched with customer data |
| `finguard2.gold.transactions` | Streaming Table | High-value transaction alerts where amount exceeds customer's transaction limit |
| `fraud_email_alert_sink` | ForEachBatch Sink | Sends fraud alert emails to customers via SMTP (Gmail) for each detected fraud |
| `email_alert_sink` | ForEachBatch Sink | Sends high-value transaction alert emails to customers via SMTP (Gmail) |

### Email Notifications

Both email sinks use a `ForEachBatch Sink` pattern with `@dp.append_flow` to stream gold-layer alerts into real-time email notifications:
- **Fraud alerts** include full transaction details, watchlist match info, risk level, and recommended action
- **High-value alerts** include transaction amount vs. customer limit, merchant, and location

Emails are sent via Gmail SMTP using credentials stored in Databricks secrets.

## Dashboard

The pipeline includes an AI/BI Lakeview dashboard (`Dashboard/FinGuard Fraud Detection & Transaction Monitoring.lvdash.json`) with 4 pages:

1. **Real-Time Monitoring** — Transaction volume counters, active fraud/high-value alert counters, alert severity bar chart, transactions-per-minute line chart, alert distribution pie chart
2. **Transaction Analysis** — Merchant category volumes, payment channel breakdown, international vs domestic, amount distribution, top 10 merchants
3. **Fraud Detection Insights** — Geographic fraud hotspots, top watchlist matches, device-based alerts, fraud alerts over time
4. **Customer Risk Analysis** — Risk score distribution, high-risk customers table, customer segment analysis

## Prerequisites

* **Databricks Secrets Scope** — `finguard-scope` with keys:
  * `kafka_connection_details` — JSON with `bootstrap_servers`, `api_key`, `api_secret`, `topic`
  * `gmail_api_key` — Gmail app password for SMTP email sending
* **Kafka** — Confluent Cloud (or compatible) with SASL_SSL / PLAIN auth
* **UC Volume** — `/Volumes/finguard2/source/fraud_watchlist/source_data/` for JSON watchlist files
* **Customers table** — `finguard2.silver.customers` must exist (populated by separate pipeline)
* **Unity Catalog** — `finguard2` catalog with `bronze`, `silver`, and `gold` schemas

## Project Structure

```
Finguard/
├── bronze/
│   ├── transactions_bronze.py          # Kafka streaming ingestion
│   └── fraud_watchlist_bronze.py       # Auto Loader JSON ingestion
├── silver/
│   ├── transactions_silver.py          # JSON parsing + data quality
│   └── fraud_watchlist_silver.py       # Cleaning and normalization
├── gold/
│   ├── transaction_count_by_minute.py           # Tumbling window aggregation
│   ├── transaction_count_by_minute_sliding_window.py  # Sliding window aggregation
│   ├── fraud_card_alert.py             # Fraud detection (stream-stream join)
│   ├── fraud_card_alert_email.py       # Fraud email notification sink
│   ├── high_value_transactions_alert.py # High-value transaction detection
│   └── high_value_transactions_email.py # High-value email notification sink
└── Dashboard/
    └── FinGuard Fraud Detection & Transaction Monitoring.lvdash.json
```

## Running the Pipeline

1. Ensure all prerequisites (secrets, Kafka, UC volume, customers table) are configured
2. Open the pipeline in the Databricks Lakeflow Pipeline Editor
3. Start a pipeline update (triggered mode for development, or set up scheduled runs)
4. Monitor the dashboard for real-time fraud detection and transaction monitoring
