"""
alert_consumer.py — Reads from fraud-alerts Kafka topic and prints alerts
"""

import os
import json
import time

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
ALERT_TOPIC  = "fraud-alerts"
SCORED_TOPIC = "scored-transactions"


def create_consumer(topic, group_id):
    """Keep retrying until Kafka is ready."""
    from kafka import KafkaConsumer

    retries = 0
    while True:
        try:
            print(f"[Consumer] Connecting to Kafka at {KAFKA_BROKER} ... (attempt {retries+1})")
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                group_id=group_id,
                consumer_timeout_ms=-1,
                request_timeout_ms=30000,
                api_version_auto_timeout_ms=30000,
            )
            print(f"[Consumer] Connected! Listening on '{topic}' ...")
            return consumer
        except Exception as e:
            retries += 1
            print(f"[Consumer] Kafka not ready: {e}. Retrying in 5s ...")
            time.sleep(5)


def consume_alerts():
    consumer = create_consumer(ALERT_TOPIC, "alert-monitor-group")
    for msg in consumer:
        rec = msg.value
        print("\n" + "=" * 50)
        print("  *** FRAUD ALERT DETECTED ***")
        print(f"  Transaction : {rec.get('transaction_id')}")
        print(f"  User        : {rec.get('user_id')}")
        print(f"  Amount      : ${rec.get('amount')}")
        print(f"  Country     : {rec.get('country')}")
        print(f"  Merchant    : {rec.get('merchant_cat')}")
        print(f"  Score       : {rec.get('fraud_score')}")
        print(f"  Time        : {rec.get('timestamp')}")
        print("=" * 50)


def consume_scored():
    consumer = create_consumer(SCORED_TOPIC, "scored-monitor-group")
    for msg in consumer:
        rec = msg.value
        flag = "FRAUD" if rec.get("is_flagged") else "OK"
        print(
            f"  [{flag}] user={rec.get('user_id')} | "
            f"amount=${rec.get('amount')} | "
            f"score={rec.get('fraud_score')} | "
            f"country={rec.get('country')}"
        )


if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "alerts"
    if mode == "scored":
        consume_scored()
    else:
        consume_alerts()
