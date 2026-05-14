# ⚡ Real-Time Fraud Detection Pipeline

An end-to-end, distributed streaming data pipeline built with **Apache Spark Structured Streaming** and **Apache Kafka**, fully containerized via **Docker**. 

This project simulates real-time financial transactions, processes them through a custom fraud-scoring rules engine, and immediately routes flagged transactions to an alert queue for monitoring.

---

## 🏗️ Architecture

The pipeline is completely decoupled. A Producer generates data into Kafka, Spark consumes it in micro-batches for parallel processing, and the results are pushed back into Kafka for downstream consumers.

```text
┌─────────────────────────────────────────────────────────────────┐
│                         KAFKA CLUSTER                           │
│  Topic: raw-transactions (4 partitions)                         │
└────────┬───────────────────────────────────────────────────────┘
         │ Spark reads each partition in parallel
         ▼
┌─────────────────────────────────────────────────────────────────┐
│                SPARK STRUCTURED STREAMING                       │
│                                                                 │
│  Micro-batch (every 10s)                                        │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  Kafka Partition 0 → RDD Partition 0 → Executor 1       │    │
│  │  Kafka Partition 1 → RDD Partition 1 → Executor 2       │    │
│  │  Kafka Partition 2 → RDD Partition 2 → Executor 3       │    │
│  │  Kafka Partition 3 → RDD Partition 3 → Executor 4       │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                 │
│  Per record:                                                    │
│    → Parse JSON                                                 │
│    → compute_score(amount, merchant, country, is_online)        │
│    → fraud_score >= 60 → is_flagged = True                      │
└────────┬───────────────────────────────────────────────────────┘
         │
         ├──→ Kafka: scored-transactions  (ALL records + score)
         └──→ Kafka: fraud-alerts         (ONLY flagged records)
🚨 Fraud Scoring Rules EngineFraud detection is handled by a PySpark User-Defined Function (UDF) that evaluates incoming JSON payloads against a rigid point-based matrix.RuleScore WeightConditionHigh AmountUp to +60 ptsAmount > $700 (scales with amount)Foreign Country+25 ptsOrigin NOT in US, UK, DE, FR, CA, AURisky Merchant+20 ptsCategory is luxury or online_gamingCard Not Present+10 ptsis_online = true🚨 THRESHOLD≥ 60 ptsFlagged as Fraudulent📁 Project StructurePlaintextfraud-detection/
├── producer/
│   └── transaction_producer.py  # Simulates live credit card swipes → Kafka
├── consumer/
│   ├── fraud_detector.py        # Spark Streaming pipeline (MAIN)
│   ├── fraud_rules.py           # Scoring rules engine & UDF registration
│   └── alert_consumer.py        # Listens to 'fraud-alerts' topic
└── docker-compose.yml           # Runs Zookeeper, Kafka, UI, Spark, & Python
🚀 Quick Start (Dockerized)
This entire architecture is orchestrated via Docker Compose. You do not need Spark or Python installed on your local machine to run this.

Prerequisites
Docker Desktop & Docker Compose

Step 1: Start the Infrastructure
Navigate to the root directory containing the docker-compose.yml file and start the cluster:

Bash
docker-compose up -d
This command spins up Zookeeper, Kafka, the Kafka UI, the Python Data Producer, the PySpark Streaming Engine, and the Python Alert Consumer simultaneously.

Step 2: Initialize Kafka Topics
Wait roughly 15 seconds for Kafka to initialize, then run the following commands to create the required message queues:

Bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic raw-transactions --partitions 4 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic scored-transactions --partitions 4 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic fraud-alerts --partitions 4 --replication-factor 1
Step 3: Monitor the Pipeline
You can watch the pipeline operating in real-time using Docker logs:

Watch the Spark Engine: docker logs -f spark-fraud-detector

Watch the live Fraud Alerts: docker logs -f alert-consumer

Watch the raw data Producer: docker logs -f producer

Step 4: Visual Dashboard
Open your web browser and navigate to the Kafka UI:
👉 http://localhost:9090

Navigate to the Topics tab to watch the message counts increase in real-time as Spark scores the data and isolates the fraud alerts.

🧠 Deep Dive: How Spark Streaming Operates Here
Parallel Ingestion: Kafka is configured with 4 partitions for raw-transactions. Spark automatically spawns 4 parallel readers (1 per partition).

Micro-Batching: Every 10 seconds (BATCH_INTERVAL), Spark collects all new records into a single micro-batch. This batch is represented as an RDD with 4 partitions.

Execution DAG: Spark builds a Directed Acyclic Graph (DAG) for the transformations: from_json → compute_score UDF → filter → to_json.

Task Scheduling: The Task Scheduler creates 4 tasks (one for each partition). Because the cluster runs with --master local[4], exactly 4 executor threads process the data simultaneously.

Continuous Loop: Results are flushed to the scored-transactions and fraud-alerts output topics, and the 10-second cycle instantly repeats.

📈 Scaling & Tuning Tips
Increase Parallelism: Increase Kafka partitions (e.g., to 8) and adjust Spark's --conf spark.sql.shuffle.partitions=8 and --master local[8] to process more data concurrently.

Lower Latency: Reduce BATCH_INTERVAL (minimum ~1s for Kafka sink) to process events closer to true real-time.

Higher Throughput: Increase maxOffsetsPerTrigger in the Spark read stream to process larger chunks of data per micro-batch.

Production Deployment: Swap --master local[*]  for a dedicated cluster manager like YARN, Mesos, or Kubernetes.