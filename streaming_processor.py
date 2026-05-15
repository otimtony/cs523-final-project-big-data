from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, when, avg
from pyspark.sql.types import StructType, StructField, StringType, LongType

schema = StructType([
    StructField("id", LongType(), True),
    StructField("type", StringType(), True),
    StructField("title", StringType(), True),
    StructField("user", StringType(), True),
    StructField("bot", StringType(), True),
    StructField("namespace", LongType(), True),
    StructField("timestamp", LongType(), True)   # Unix epoch in milliseconds
])

spark = SparkSession.builder \
    .appName("WikimediaStreamProcessor") \
.config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoint") \
    .enableHiveSupport() \
    .getOrCreate()

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-server:9092") \
    .option("subscribe", "wikimedia-events") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

df_with_time = df_parsed.withColumn("event_time", (col("timestamp") / 1000).cast("timestamp"))

windowed_counts = df_with_time \
    .withWatermark("event_time", "1 minute") \
    .groupBy(window(col("event_time"), "1 minute", "30 seconds"), col("namespace")) \
    .agg(count("*").alias("change_count"))

user_counts = df_with_time \
    .withWatermark("event_time", "1 minute") \
    .groupBy(window(col("event_time"), "1 minute"), col("user")) \
    .agg(count("*").alias("user_edit_count"))

anomalies = user_counts.filter(col("user_edit_count") > 5) \
    .select("user", "window", "user_edit_count") \
    .withColumn("anomaly", when(col("user_edit_count") > 5, True).otherwise(False))

query_console = windowed_counts.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query_anomalies = anomalies.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query_console.awaitTermination()
