from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import os
import logging

# Environment variables
KAFKA_TOPIC_NAME_CONS = os.environ.get('INPUT_TOPIC')
KAFKA_OUTPUT_TOPIC_NAME_CONS = os.environ.get('OUTPUT_TOPIC')
KAFKA_BOOTSTRAP_SERVERS_CONS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS')

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Spark session setup with Kafka and MongoDB connectors
spark = SparkSession.builder \
    .appName("streaming_interaction") \
    .config('spark.jars.packages', 
            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,' 
            'org.mongodb.spark:mongo-spark-connector_2.12:10.1.1') \
    .getOrCreate()

# Set log level for Spark
spark.sparkContext.setLogLevel("INFO")

# Define schema for incoming messages
schema = StructType().add("interaction_type", StringType()).add("timestamp", TimestampType()).add("user_id", DoubleType()).add("item_id", DoubleType())


# Read from Kafka stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
    .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
    .option("startingOffsets", "latest") \
    .load()

logger.info("Reading incoming data...")

# Parse the Kafka message (value column) as a string
raw_messages = df.selectExpr("CAST(value AS STRING)", "timestamp")

# Log schema for debugging
logger.info("Schema of raw messages: %s", raw_messages.printSchema())

# Parse JSON to structured format
messages = raw_messages \
    .select(from_json(col("value"), schema).alias("value_columns"), "timestamp") \
    .select("value_columns.*", "timestamp")

# Log schema after parsing
logger.info("Parsed messages schema: %s", messages.printSchema())

# Write to MongoDB
query = messages.writeStream \
    .format("mongodb") \
    .queryName("ToMDB") \
    .option("checkpointLocation", "/tmp/pyspark5/") \
    .option("forceDeleteTempCheckpointLocation", "true") \
    .option('spark.mongodb.connection.uri', 'mongodb://admin:adminpassword@mongo:27017/?authSource=admin') \
    .option('spark.mongodb.database', 'test') \
    .option('spark.mongodb.collection', 'my_collection') \
    .trigger(processingTime="10 seconds") \
    .outputMode("append") \
    .start()

# Wait for the query to terminate
query.awaitTermination()
