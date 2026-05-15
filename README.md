# CS523 Final Project — Wikimedia Big Data Pipeline

Real-time analytics pipeline that ingests live Wikipedia edit events, processes them with Spark Structured Streaming, stores results in HBase and Hive, and visualises them on a live web dashboard.

---

## Architecture

```
Wikimedia WebSocket (wss://stream.wikimedia.org/v2/stream/recentchange)
        │
        ▼
wikimedia_producer.py          [Python — kafka-python + websocket-client]
        │  topic: wikimedia-events
        ▼
Apache Kafka 3.x               [broker: localhost:9092]
        │
        ▼
streaming_processor.py         [PySpark Structured Streaming]
   ├── windowed_counts          (1 min / 30 sec slide, grouped by namespace)
   ├── user_counts              (1 min tumbling, grouped by user)
   └── anomalies                (users with > 5 edits/min flagged)
        │
        ├──────────────────────────────────────────┐
        ▼                                          ▼
HBase 2.x (wikimedia_raw)              HDFS Parquet files
hbase_writer.js                        hdfs:///user/hadoop/wikimedia/
[Node.js — kafkajs + node-fetch]         ├── windowed_counts/
                                         └── anomalies/
                                                   │
                                                   ▼
                                         Apache Hive 3.x (wikimedia_db)
                                         hive_schema.sql
        │
        ▼
dashboard/server.js            [Node.js — Express on port 3000]
        │  reads HBase via REST (localhost:8080)
        ▼
http://localhost:3000          [Chart.js dashboard — index.html]

── BONUS ──────────────────────────────────────────────────────────────────────
bonus/spark_sql_join.py        [PySpark SQL — enriches windowed_counts with
                                namespace_labels.csv via LEFT JOIN + RANK()]
                                saves → wikimedia_db.enriched_activity
                                        wikimedia_db.anomaly_summary
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python 3.x — kafka-python, websocket-client |
| Message broker | Apache Kafka 3.x with ZooKeeper |
| Stream processing | Apache Spark 3.4 + PySpark Structured Streaming |
| NoSQL storage | Apache HBase 2.x — REST/Stargate on port 8080 |
| Data warehouse | Apache Hive 3.x with HDFS Parquet |
| Storage writer | Node.js 18+ — kafkajs, node-fetch |
| Dashboard API | Node.js 18+ — Express, cors, node-fetch |
| Frontend | Vanilla HTML/JS + Chart.js (CDN, no build step) |

---

## Prerequisites

Ensure all of the following services are installed and accessible:

- Apache ZooKeeper
- Apache Kafka 3.x (`bin/` scripts on `$PATH`)
- Apache Spark 3.4 with PySpark (`spark-submit` on `$PATH`)
- Apache HBase 2.x with REST/Stargate server enabled
- Apache Hive 3.x (`hive` CLI on `$PATH`)
- HDFS running and accessible (`hdfs` CLI on `$PATH`)
- Python 3.x
- Node.js 18+

---

## Setup

### Step 1 — Install Python dependencies
```bash
pip install kafka-python websocket-client pyspark requests
```

### Step 2 — Install Node.js dependencies
```bash
cd storage && npm install
cd ../dashboard && npm install
```

### Step 3 — Create Kafka topic
```bash
bin/kafka-topics.sh --create --topic wikimedia-events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### Step 4 — Create HBase tables (run inside `hbase shell`)
```
create 'wikimedia_counts', 'stats'
create 'wikimedia_raw', 'event'
```

### Step 5 — Create HDFS directories
```bash
hdfs dfs -mkdir -p /user/hadoop/wikimedia/windowed_counts
hdfs dfs -mkdir -p /user/hadoop/wikimedia/anomalies
hdfs dfs -mkdir -p /user/hadoop/reference
hdfs dfs -put bonus/namespace_labels.csv /user/hadoop/reference/
```

### Step 6 — Initialise Hive schema
```bash
hive -f storage/hive_schema.sql
```

---

## Starting the Pipeline

Open **6 terminals** and run each command in order:

| Terminal | Command |
|---|---|
| 1 | `bin/zookeeper-server-start.sh config/zookeeper.properties` |
| 2 | `bin/kafka-server-start.sh config/server.properties` |
| 3 | `python producer/wikimedia_producer.py` |
| 4 | `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 spark/streaming_processor.py` |
| 5 | `cd storage && node hbase_writer.js` |
| 6 | `cd dashboard && node server.js` |

Dashboard: **http://localhost:3000**

---

## Bonus — Spark SQL Enrichment

Upload the namespace reference CSV and run the enrichment job:

```bash
hdfs dfs -put bonus/namespace_labels.csv /user/hadoop/reference/
spark-submit bonus/spark_sql_join.py
```

This joins `windowed_counts` with `namespace_labels.csv` using a `LEFT JOIN` and window `RANK()`, then saves the enriched results as Hive tables:

- `wikimedia_db.enriched_activity`
- `wikimedia_db.anomaly_summary`
