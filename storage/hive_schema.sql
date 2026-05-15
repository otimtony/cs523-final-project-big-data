-- ─────────────────────────────────────────────────────────────────────────────
-- CS523 Final Project — Hive Schema
-- Creates external Parquet tables that mirror the Spark streaming output paths
-- ─────────────────────────────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS wikimedia_db;

USE wikimedia_db;

-- ─── Table 1: windowed_counts ─────────────────────────────────────────────────
-- Matches the windowed_counts stream written by streaming_processor.py
CREATE EXTERNAL TABLE IF NOT EXISTS windowed_counts (
    window_start  TIMESTAMP,
    window_end    TIMESTAMP,
    namespace     BIGINT,
    change_count  BIGINT
)
STORED AS PARQUET
LOCATION 'hdfs:///user/hadoop/wikimedia/windowed_counts';

-- ─── Table 2: anomalies ───────────────────────────────────────────────────────
-- Matches the anomalies stream written by streaming_processor.py
CREATE EXTERNAL TABLE IF NOT EXISTS anomalies (
    `user`           STRING,
    window_start     TIMESTAMP,
    window_end       TIMESTAMP,
    user_edit_count  BIGINT,
    anomaly          BOOLEAN
)
STORED AS PARQUET
LOCATION 'hdfs:///user/hadoop/wikimedia/anomalies';

-- ─────────────────────────────────────────────────────────────────────────────
-- Sample Query 1: Top 10 most active namespaces across all windows
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    namespace,
    SUM(change_count) AS total_edits
FROM windowed_counts
GROUP BY namespace
ORDER BY total_edits DESC
LIMIT 10;

-- ─────────────────────────────────────────────────────────────────────────────
-- Sample Query 2: Recent anomalous users ordered by latest window first
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    `user`,
    window_start,
    window_end,
    user_edit_count,
    anomaly
FROM anomalies
WHERE anomaly = TRUE
ORDER BY window_start DESC
LIMIT 20;
