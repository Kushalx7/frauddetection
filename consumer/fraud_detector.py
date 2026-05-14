"""
fraud_detector.py — Spark Structured Streaming fraud detection pipeline
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType,
)

# ── NEW: Import your external rules ──────────────────────────────────────────
from fraud_rules import register_udfs

# ── CONFIG ───────────────────────────────────────────────────────────────────
KAFKA_BROKER          = os.getenv("KAFKA_BROKER", "localhost:9092")
INPUT_TOPIC           = "raw-transactions"
OUTPUT_TOPIC_SCORED   = "scored-transactions"
OUTPUT_TOPIC_ALERTS   = "fraud-alerts"
CHECKPOINT_DIR        = "/tmp/spark-checkpoints/fraud-detector"
BATCH_INTERVAL        = "10 seconds"
WATERMARK_DELAY       = "2 minutes"
WINDOW_DURATION       = "1 minute"
VELOCITY_THRESHOLD    = 5
FRAUD_THRESHOLD       = 60.0

# ── SCHEMA ───────────────────────────────────────────────────────────────────
TXN_SCHEMA = StructType([
    StructField("transaction_id", StringType(),  True),
    StructField("user_id",        StringType(),  True),
    StructField("amount",         DoubleType(),  True),
    StructField("merchant_cat",   StringType(),  True),
    StructField("country",        StringType(),  True),
    StructField("card_type",      StringType(),  True),
    StructField("timestamp",      StringType(),  True),
    StructField("is_online",      BooleanType(), True),
    StructField("label",          StringType(),  True),
])

# (The inline compute_score UDF has been deleted from here)

# ── SPARK SESSION ─────────────────────────────────────────────────────────────
def build_spark():
    return (
        SparkSession.builder
        .appName("RealTimeFraudDetection")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

# ── READ FROM KAFKA ───────────────────────────────────────────────────────────
def read_kafka(spark):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 1000)
        .load()
    )

# ── PARSE & SCORE ─────────────────────────────────────────────────────────────
# Note: We now pass score_udf into this function
def parse_and_score(raw_df, score_udf):
    return (
        raw_df
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.from_json(F.col("value").cast("string"), TXN_SCHEMA).alias("data"),
        )
        .select("kafka_key", "data.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withColumn("fraud_score", score_udf(
            F.col("amount"),
            F.col("merchant_cat"),
            F.col("country"),
            F.col("is_online"),
        ))
        .withColumn("is_flagged", F.col("fraud_score") >= FRAUD_THRESHOLD)
    )

# ── WRITE TO KAFKA ────────────────────────────────────────────────────────────
def to_kafka_df(df, key_col):
    return (
        df
        .withColumn("value", F.to_json(F.struct(*[c for c in df.columns])))
        .withColumn("key", F.col(key_col).cast("string"))
        .select("key", "value")
    )

def write_scored(scored_df):
    return (
        to_kafka_df(scored_df, "user_id")
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("topic", OUTPUT_TOPIC_SCORED)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/scored")
        .trigger(processingTime=BATCH_INTERVAL)
        .start()
    )

def write_alerts(scored_df):
    alerts = scored_df.filter(F.col("is_flagged") == True)
    return (
        to_kafka_df(alerts, "transaction_id")
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("topic", OUTPUT_TOPIC_ALERTS)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/alerts")
        .trigger(processingTime=BATCH_INTERVAL)
        .start()
    )

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    # ── NEW: Register the UDF from fraud_rules.py using the spark session ──
    score_udf = register_udfs(spark)

    print("=" * 60)
    print("  Real-Time Fraud Detection — Spark Structured Streaming")
    print(f"  Kafka Broker : {KAFKA_BROKER}")
    print(f"  Input Topic  : {INPUT_TOPIC}")
    print(f"  Output       : {OUTPUT_TOPIC_SCORED}, {OUTPUT_TOPIC_ALERTS}")
    print(f"  Batch        : every {BATCH_INTERVAL}")
    print("=" * 60)

    raw_df    = read_kafka(spark)
    # ── NEW: Pass the score_udf into the parsing function ──
    scored_df = parse_and_score(raw_df, score_udf)

    q1 = write_scored(scored_df)
    q2 = write_alerts(scored_df)

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()