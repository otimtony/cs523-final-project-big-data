#!/usr/bin/env python
"""
CS523 Final Project — Bonus: Spark SQL Enrichment
Joins windowed_counts and anomalies Parquet data with namespace reference CSV,
then saves enriched results as Hive tables in wikimedia_db.
"""

from pyspark.sql import SparkSession

# ─── Spark session ────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("WikimediaSQLEnrich") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ─── Load reference data: namespace labels from HDFS CSV ─────────────────────
namespace_ref = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs:///user/hadoop/reference/namespace_labels.csv")

# ─── Load Parquet outputs written by streaming_processor.py ──────────────────
windowed_counts = spark.read.parquet(
    "hdfs:///user/hadoop/wikimedia/windowed_counts"
)

anomalies = spark.read.parquet(
    "hdfs:///user/hadoop/wikimedia/anomalies"
)

# ─── Register all three as SQL temp views ────────────────────────────────────
namespace_ref.createOrReplaceTempView("ns_labels")
windowed_counts.createOrReplaceTempView("windowed_counts")
anomalies.createOrReplaceTempView("anomalies")

# ─── Query 1: Enriched namespace activity with ranking ───────────────────────
print("\n=== ENRICHED NAMESPACE ACTIVITY ===")

enriched = spark.sql("""
    SELECT wc.window.start      AS window_start,
           wc.window.end        AS window_end,
           wc.namespace,
           ns.label             AS namespace_label,
           ns.description,
           ns.type              AS namespace_type,
           wc.change_count,
           RANK() OVER (
               PARTITION BY wc.window.start
               ORDER BY wc.change_count DESC
           ) AS activity_rank
    FROM   windowed_counts wc
    LEFT JOIN ns_labels ns ON wc.namespace = ns.namespace
    ORDER BY window_start DESC, change_count DESC
""")

enriched.show(30, truncate=False)

# ─── Query 2: Anomaly summary — top offenders across all flagged windows ──────
print("\n=== TOP ANOMALOUS USERS ===")

anomaly_summary = spark.sql("""
    SELECT a.user,
           MAX(a.user_edit_count)  AS peak_edits_per_min,
           COUNT(*)                AS flagged_windows,
           MIN(a.window.start)     AS first_flagged,
           MAX(a.window.start)     AS last_flagged
    FROM   anomalies a
    GROUP  BY a.user
    ORDER  BY peak_edits_per_min DESC
    LIMIT  20
""")

anomaly_summary.show(20)

# ─── Persist results as Hive tables ──────────────────────────────────────────
enriched.write \
    .mode("overwrite") \
    .saveAsTable("wikimedia_db.enriched_activity")
print("✅ Saved: wikimedia_db.enriched_activity")

anomaly_summary.write \
    .mode("overwrite") \
    .saveAsTable("wikimedia_db.anomaly_summary")
print("✅ Saved: wikimedia_db.anomaly_summary")

# ─── Done ─────────────────────────────────────────────────────────────────────
spark.stop()
