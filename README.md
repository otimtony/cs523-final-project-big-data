# cs523-final-project-big-data

Wikimedia Real-Time Analytics Pipeline
What this project does

This project is a real-time big data pipeline built for a Big Data Technologies course (CS523). It processes live Wikipedia edit events from the Wikimedia Recent Changes stream and runs them through an end-to-end data pipeline covering ingestion, processing, storage, and visualization.

Pipeline overview

The system is designed to handle streaming data reliably and efficiently using modern big data tools:

Data ingestion: Apache Kafka is used to capture and buffer real-time Wikipedia edit events in a fault-tolerant way.
Stream processing: Apache Spark Structured Streaming processes the data in real time. This includes computing windowed aggregations, detecting unusual activity patterns (anomalies), and handling late-arriving events using watermarking.
Storage layer: Processed data is stored in both Apache HBase for low-latency NoSQL access and Apache Hive for analytical querying in a data warehouse format.
Visualization: Apache Zeppelin dashboards are used to visualize streaming results in near real time, allowing interactive exploration of trends and activity.
Additional feature

A key enhancement in this project is data enrichment through Spark SQL. The live Wikipedia stream is joined with a static CSV dataset containing namespace metadata. This demonstrates how batch data can be combined with streaming data to improve context and analytics
