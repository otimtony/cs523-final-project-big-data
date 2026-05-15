# CS523 Big Data Technologies — Full Setup & Run Guide

Complete, step-by-step instructions for standing up the Docker lab environment and running the Wikimedia real-time analytics pipeline.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Prerequisites](#2-prerequisites)
3. [Project Structure](#3-project-structure)
4. [Step 1 — Start the Docker Environment](#step-1--start-the-docker-environment)
5. [Step 2 — Enter the Lab Container](#step-2--enter-the-lab-container)
6. [Step 3 — Install Python Dependencies](#step-3--install-python-dependencies)
7. [Step 4 — Install Node.js Dependencies](#step-4--install-nodejs-dependencies)
8. [Step 5 — Create the Kafka Topic](#step-5--create-the-kafka-topic)
9. [Step 6 — Create HBase Tables](#step-6--create-hbase-tables)
10. [Step 7 — Create HDFS Directories](#step-7--create-hdfs-directories)
11. [Step 8 — Initialise the Hive Schema](#step-8--initialise-the-hive-schema)
12. [Step 9 — Run the Pipeline](#step-9--run-the-pipeline)
13. [Step 10 — Access Web UIs & Dashboard](#step-10--access-web-uis--dashboard)
14. [Step 11 (Bonus) — Spark SQL Enrichment](#step-11-bonus--spark-sql-enrichment)
15. [Stopping the Environment](#stopping-the-environment)
16. [Connection Reference](#connection-reference)
17. [Troubleshooting](#troubleshooting)

---

## 1. Architecture Overview

```
Wikimedia SSE stream (https://stream.wikimedia.org/v2/stream/recentchange)
        │
        ▼
wikimedia_producer.py          [Python — requests, kafka-python]
        │  topic: wikimedia-events
        ▼
Apache Kafka 3.4               [broker: kafka-server:9092]
        │
        ▼
streaming_processor.py         [PySpark Structured Streaming]
   ├── windowed_counts          (1 min window / 30 sec slide, by namespace)
   ├── user_counts              (1 min tumbling window, by user)
   └── anomalies                (users with > 5 edits/min)
        │
        ├─────────────────────────────────────────────────┐
        ▼                                                 ▼
HBase 2.x (wikimedia_raw / wikimedia_counts)     HDFS Parquet files
hbase_writer.js [Node.js — kafkajs, node-fetch]  hdfs:///user/hadoop/wikimedia/
                                                    ├── windowed_counts/
                                                    └── anomalies/
                                                              │
                                                              ▼
                                                    Apache Hive 3.x (wikimedia_db)
                                                    hive_schema.sql
        │
        ▼
dashboard/server.js  [Node.js Express — port 3000]
        │  reads HBase REST (localhost:8080)
        ▼
http://localhost:3000  [Chart.js live dashboard]

── BONUS ──────────────────────────────────────────────────────────────────────
bonus/spark_sql_join.py   [PySpark SQL — enriches windowed_counts with
                           namespace_labels.csv via LEFT JOIN + RANK()]
                           saves → wikimedia_db.enriched_activity
                                   wikimedia_db.anomaly_summary
```

---

## 2. Prerequisites

| Requirement | Notes |
|---|---|
| [Docker Desktop](https://www.docker.com/products/docker-desktop/) | Must be running before any step |
| 8 GB RAM allocated to Docker | Recommended minimum for Hadoop + Spark + HBase |
| Internet access | Required by the Wikimedia producer to stream live events |
| Lab password | `DEcs523bdt_Feb2026` (set in `docker-compose.yml` as `CLASS_PASS`) |

**Windows users only** — if you run Spark/Hadoop locally (outside Docker):
1. Create `C:\hadoop\bin`
2. Copy `winutils.exe` and `hadoop.dll` (included in this repo) into that folder
3. Set environment variable `HADOOP_HOME=C:\hadoop`
4. Add `%HADOOP_HOME%\bin` to your system `PATH`

---

## 3. Project Structure

```
cs523-bdt/
├── docker-compose.yml                   # All four Docker services
├── hadoop.dll                           # Windows Hadoop native library
├── winutils.exe                         # Windows Hadoop utility
└── my_code/
    └── cs523-final-project-big-data/
        ├── wikimedia_producer.py        # Kafka producer (SSE → Kafka)
        ├── streaming_processor.py       # PySpark streaming job
        ├── storage/
        │   ├── hbase_writer.js          # Kafka → HBase writer (Node.js)
        │   ├── hive_schema.sql          # Hive DDL + sample queries
        │   ├── package.json
        │   └── package-lock.json
        ├── dashboard/
        │   ├── server.js                # Express API + static server
        │   ├── package.json
        │   └── package-lock.json
        └── bonus/
            ├── spark_sql_join.py        # Spark SQL enrichment job
            └── namespace_labels.csv     # Namespace reference data
```

> **Note:** The `my_code/` directory is mounted inside the container at `/opt/my_code`, so any edits you make on your host machine are immediately visible inside the container.

---

## Step 1 — Start the Docker Environment

Open a terminal in the project root (where `docker-compose.yml` lives) and run:

```bash
docker-compose up -d
```

This pulls and starts four containers:

| Container | Role | Startup time |
|---|---|---|
| `zookeeper-server` | Coordination for Kafka and HBase | ~10 s |
| `kafka-server` | Message broker | ~15 s |
| `hive-metastore-db` | PostgreSQL backing Hive Metastore | ~10 s |
| `cs523bdt-lab` | Hadoop, Hive, HBase, Spark, Pig, Flume, Sqoop | ~60–90 s |

Wait ~90 seconds before proceeding. You can watch the lab container start with:

```bash
docker logs -f cs523bdt-lab
```

Press `Ctrl+C` to stop following logs once you see services ready.

---

## Step 2 — Enter the Lab Container

All pipeline steps (Steps 3–11) run **inside** the `cs523bdt-lab` container. Open a shell into it:

```bash
docker exec -it cs523bdt-lab /bin/bash
```

Your project code is at `/opt/my_code/cs523-final-project-big-data/` inside the container.

```bash
cd /opt/my_code/cs523-final-project-big-data
```

---

## Step 3 — Install Python Dependencies

```bash
pip install kafka-python requests pyspark
```

Verify:

```bash
python3 -c "from kafka import KafkaProducer; print('kafka-python OK')"
```

---

## Step 4 — Install Node.js Dependencies

Install dependencies for both the HBase writer and the dashboard:

```bash
cd /opt/my_code/cs523-final-project-big-data/storage
npm install

cd /opt/my_code/cs523-final-project-big-data/dashboard
npm install
```

---

## Step 5 — Create the Kafka Topic

```bash
kafka-topics.sh --create \
  --topic wikimedia-events \
  --bootstrap-server kafka-server:9092 \
  --partitions 3 \
  --replication-factor 1
```

Confirm the topic was created:

```bash
kafka-topics.sh --list --bootstrap-server kafka-server:9092
```

You should see `wikimedia-events` in the output.

---

## Step 6 — Create HBase Tables

Open the HBase shell:

```bash
hbase shell
```

Inside the shell, run:

```
create 'wikimedia_counts', 'stats'
create 'wikimedia_raw', 'event'
list
exit
```

You should see both tables listed after `list`.

---

## Step 7 — Create HDFS Directories

```bash
hdfs dfs -mkdir -p /user/hadoop/wikimedia/windowed_counts
hdfs dfs -mkdir -p /user/hadoop/wikimedia/anomalies
hdfs dfs -mkdir -p /user/hadoop/reference

# Upload the namespace reference CSV for the bonus step
hdfs dfs -put /opt/my_code/cs523-final-project-big-data/bonus/namespace_labels.csv \
  /user/hadoop/reference/
```

Verify:

```bash
hdfs dfs -ls /user/hadoop/wikimedia/
hdfs dfs -ls /user/hadoop/reference/
```

---

## Step 8 — Initialise the Hive Schema

```bash
hive -f /opt/my_code/cs523-final-project-big-data/storage/hive_schema.sql
```

This creates the `wikimedia_db` database and two external Parquet tables (`windowed_counts` and `anomalies`) pointing at the HDFS paths created in Step 7. It also runs two sample queries (output may be empty until data flows in Step 9).

---

## Step 9 — Run the Pipeline

The pipeline requires **5 processes running simultaneously**. Open a separate terminal tab/window for each, then `exec` into the container each time:

```bash
# In each new terminal:
docker exec -it cs523bdt-lab /bin/bash
```

### Terminal 1 — Wikimedia Kafka Producer

Streams live Wikipedia edit events into Kafka:

```bash
python3 /opt/my_code/cs523-final-project-big-data/wikimedia_producer.py
```

Expected output:
```
Connecting to https://stream.wikimedia.org/v2/stream/recentchange ...
Connected. Streaming events...
Sent: Talk:Some_Wikipedia_Article
Sent: User:SomeEditor
...
```

### Terminal 2 — PySpark Structured Streaming Processor

Reads from Kafka and writes windowed aggregations to console (and HDFS once configured):

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  /opt/my_code/cs523-final-project-big-data/streaming_processor.py
```

Expected output (every 30 seconds):
```
-------------------------------------------
Batch: 1
-------------------------------------------
+--------------------+---------+------------+
|window              |namespace|change_count|
+--------------------+---------+------------+
|{2026-05-14 ..., ...}|0       |42          |
...
```

### Terminal 3 — HBase Writer (Node.js)

Consumes from Kafka and writes raw events to HBase:

```bash
cd /opt/my_code/cs523-final-project-big-data/storage
node hbase_writer.js
```

Expected output:
```
Connecting to Kafka broker...
[KAFKA] Connected. Subscribing to topic: wikimedia-events
[HBASE] Wrote: NS0_1715123456789_12345
...
```

### Terminal 4 — Dashboard Server (Node.js)

Serves the live dashboard on port 3000, reading from HBase REST:

```bash
cd /opt/my_code/cs523-final-project-big-data/dashboard
node server.js
```

Expected output:
```
Dashboard running at http://localhost:3000
```

### Terminal 5 — (Optional) Monitor Kafka messages

To observe raw messages flowing through Kafka:

```bash
kafka-console-consumer.sh \
  --bootstrap-server kafka-server:9092 \
  --topic wikimedia-events \
  --from-beginning
```

---

## Step 10 — Access Web UIs & Dashboard

Once the pipeline is running, open these URLs in your browser on the **host machine**:

| Service | URL | Description |
|---|---|---|
| **Live Dashboard** | http://localhost:3000 | Chart.js charts — namespace counts, recent events, stats |
| **HDFS NameNode UI** | http://localhost:9870 | Browse HDFS, check file sizes |
| **YARN Resource Manager** | http://localhost:8088 | Monitor running Spark jobs |
| **HBase Master UI** | http://localhost:16010 | Table stats and region servers |
| **Spark Job UI** | http://localhost:4040 | Active streaming query metrics |
| **HBase REST (Stargate)** | http://localhost:8080 | Raw REST API used by Node.js apps |

---

## Step 11 (Bonus) — Spark SQL Enrichment

After the streaming processor has been running for at least a few minutes (so that Parquet files exist in HDFS), run the enrichment job:

```bash
spark-submit \
  /opt/my_code/cs523-final-project-big-data/bonus/spark_sql_join.py
```

This job:
1. Loads `windowed_counts` and `anomalies` Parquet files from HDFS
2. Joins them with `namespace_labels.csv` using a `LEFT JOIN`
3. Applies `RANK()` to order namespaces by activity within each time window
4. Saves results as two Hive tables:
   - `wikimedia_db.enriched_activity`
   - `wikimedia_db.anomaly_summary`

Query the results in Hive:

```bash
hive
```

```sql
USE wikimedia_db;
SELECT * FROM enriched_activity LIMIT 20;
SELECT * FROM anomaly_summary LIMIT 20;
```

---

## Stopping the Environment

To stop all containers (preserves Hive Metastore volume data):

```bash
docker-compose down
```

To stop and **delete all data** (including the Hive Metastore PostgreSQL volume):

```bash
docker-compose down -v
```

---

## Connection Reference

| Service | Host (inside container) | Host (from your machine) |
|---|---|---|
| Kafka Broker | `kafka-server:9092` | `localhost:9092` |
| ZooKeeper | `zookeeper-server:2181` | `localhost:2181` |
| HBase REST | `localhost:8080` | `localhost:8080` |
| Hive JDBC | `localhost:10000` | `localhost:10000` |
| PostgreSQL (Hive Metastore) | `hive-metastore-db:5432` | `localhost:5432` |
| Postgres credentials | user: `hive` / password: `hivepassword` / db: `hive_metastore` | same |
| Lab container password | `DEcs523bdt_Feb2026` | — |

---

## Troubleshooting

**Container won't start / exits immediately**
- Ensure Docker Desktop is running and has at least 8 GB RAM allocated (Docker Desktop → Settings → Resources)
- Run `docker-compose logs cs523bdt-lab` to see the startup error

**Kafka topic creation fails: `Connection refused`**
- The Kafka broker may still be starting. Wait 30 seconds and retry.
- Verify Kafka is up: `docker ps | grep kafka-server`

**`spark-submit` fails with `ClassNotFoundException` for Kafka connector**
- The `--packages` flag downloads the Spark-Kafka connector on first run. Ensure you have internet access inside the container.
- If downloads are blocked, check if the jar is already bundled: `find / -name "spark-sql-kafka*" 2>/dev/null`

**HBase write errors: `HTTP 404` or `connection refused`**
- HBase REST (Stargate) may not have started yet. Check: `curl http://localhost:8080/version`
- If it fails, HBase services may still be initialising. Wait and retry.

**Dashboard shows no data**
- Data only appears after the producer, Spark processor, and HBase writer are all running and have processed at least one batch (~30–60 seconds).
- Check `/api/recent` directly: `curl http://localhost:3000/api/recent`

**`pip install` fails with SSL errors**
- Run: `pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org kafka-python requests pyspark`

**HDFS commands fail with `Connection refused`**
- HDFS NameNode may still be starting. Check the HDFS UI at http://localhost:9870 or wait 30 seconds and retry.

**`hdfs dfs -put` for namespace_labels.csv fails**
- Make sure you ran Step 7 to create the `/user/hadoop/reference` directory first.
- Confirm the file exists in the container: `ls /opt/my_code/cs523-final-project-big-data/bonus/namespace_labels.csv`
