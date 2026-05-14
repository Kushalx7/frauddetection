"""
Transaction Producer — simulates real-time bank transactions
Publishes to Kafka topic: raw-transactions
"""

import os
import json
import random
import time
import uuid
from datetime import datetime

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC        = "raw-transactions"

USER_IDS      = [f"USER_{i:04d}" for i in range(1, 201)]
MERCHANT_CATS = ["groceries", "electronics", "travel", "restaurant",
                 "fuel", "pharmacy", "luxury", "online_gaming"]
COUNTRIES     = ["US", "US", "US", "US", "UK", "IN", "DE", "BR", "NG", "RU"]
CARD_TYPES    = ["VISA", "MASTERCARD", "AMEX"]


def generate_normal_transaction(user_id):
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id":        user_id,
        "amount":         round(random.uniform(5.0, 300.0), 2),
        "merchant_cat":   random.choice(["groceries", "restaurant", "fuel", "pharmacy"]),
        "country":        "US",
        "card_type":      random.choice(CARD_TYPES),
        "timestamp":      datetime.utcnow().isoformat(),
        "is_online":      random.choice([True, False]),
        "label":          "normal",
    }


def generate_fraudulent_transaction(user_id):
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id":        user_id,
        "amount":         round(random.uniform(800.0, 9999.0), 2),
        "merchant_cat":   random.choice(["luxury", "online_gaming", "electronics"]),
        "country":        random.choice(["NG", "RU", "BR"]),
        "card_type":      random.choice(CARD_TYPES),
        "timestamp":      datetime.utcnow().isoformat(),
        "is_online":      True,
        "label":          "fraud",
    }


def create_producer():
    """Keep retrying until Kafka is ready."""
    from kafka import KafkaProducer
    import kafka.errors as Errors

    retries = 0
    while True:
        try:
            print(f"[Producer] Connecting to Kafka at {KAFKA_BROKER} ... (attempt {retries+1})")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                request_timeout_ms=30000,
                api_version_auto_timeout_ms=30000,
                max_block_ms=30000,
            )
            print(f"[Producer] Connected to Kafka successfully!")
            return producer
        except Exception as e:
            retries += 1
            print(f"[Producer] Kafka not ready yet: {e}")
            print(f"[Producer] Retrying in 5 seconds ...")
            time.sleep(5)


def produce():
    producer = create_producer()
    print(f"[Producer] Publishing to '{TOPIC}' ...")
    count = 0
    try:
        while True:
            user_id = random.choice(USER_IDS)

            if random.random() < 0.10:
                txn = generate_fraudulent_transaction(user_id)
            else:
                txn = generate_normal_transaction(user_id)

            producer.send(TOPIC, key=user_id, value=txn)
            count += 1

            if count % 50 == 0:
                print(f"[Producer] Sent {count} transactions | last: {txn['user_id']} ${txn['amount']} {txn['country']}")

            time.sleep(random.uniform(0.05, 0.3))

    except KeyboardInterrupt:
        print(f"\n[Producer] Stopped. Total sent: {count}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    produce()
