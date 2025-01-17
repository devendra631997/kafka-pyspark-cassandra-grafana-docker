from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, DoubleType, StructType
import os
import logging

KAFKA_TOPIC_NAME_CONS = os.environ.get('INPUT_TOPIC')
KAFKA_OUTPUT_TOPIC_NAME_CONS = os.environ.get('OUTPUT_TOPIC')
KAFKA_BOOTSTRAP_SERVERS_CONS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS')

logging.basicConfig(level=logging.INFO)

spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka Demo") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

schema = StructType().add("interaction_type", StringType()).add("timestamp", StringType()).add("user_id", DoubleType()).add("item_id", DoubleType())

df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .option("startingOffsets", "latest") \
        .load()

logging.info("Reading incoming data...")

# simply converting the 'value' column from json to a string and keeping the 'timestamp' column as is
raw_messages = df.selectExpr("CAST(value AS STRING)", "timestamp")
logging.info("raw_messages..............................................................................................", raw_messages)

messages = raw_messages.select(from_json(col("value"), schema).alias("value_columns"), "timestamp")

logging.info("messages..............................................................................................", messages)


query = messages.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()


query.awaitTermination()
