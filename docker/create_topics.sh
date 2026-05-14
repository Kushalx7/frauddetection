#!/bin/bash
# create_topics.sh — Create all required Kafka topics

BROKER="localhost:9092"
PARTITIONS=4
REPLICATION=1

echo "Creating Kafka topics..."

kafka-topics.sh --create \
  --bootstrap-server $BROKER \
  --topic raw-transactions \
  --partitions $PARTITIONS \
  --replication-factor $REPLICATION \
  --if-not-exists

kafka-topics.sh --create \
  --bootstrap-server $BROKER \
  --topic scored-transactions \
  --partitions $PARTITIONS \
  --replication-factor $REPLICATION \
  --if-not-exists

kafka-topics.sh --create \
  --bootstrap-server $BROKER \
  --topic fraud-alerts \
  --partitions $PARTITIONS \
  --replication-factor $REPLICATION \
  --if-not-exists

echo ""
echo "Topics created:"
kafka-topics.sh --list --bootstrap-server $BROKER
