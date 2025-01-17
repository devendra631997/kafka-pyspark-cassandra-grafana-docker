from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, DoubleType, StructType, TimestampType
import os
import logging

# Environment variables
KAFKA_TOPIC_NAME_CONS = os.environ.get('INPUT_TOPIC')
KAFKA_OUTPUT_TOPIC_NAME_CONS = os.environ.get('OUTPUT_TOPIC')
KAFKA_BOOTSTRAP_SERVERS_CONS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS')

# Setup logging
logging.basicConfig(level=logging.INFO)

# Spark session setup with Kafka and MongoDB connectors
spark = SparkSession.\
        builder.\
        appName("streamingExampleWrite").\
        config('spark.jars.packages', 
               'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,' 
               'org.mongodb.spark:mongo-spark-connector_2.12:10.1.1').\
        getOrCreate()

spark.sparkContext.setLogLevel("INFO")

# Define the schema for incoming messages
schema = StructType().add("interaction_type", StringType()).add("timestamp", TimestampType()).add("user_id", DoubleType()).add("item_id", DoubleType())

# Read from Kafka stream
df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .option("startingOffsets", "latest") \
        .load()

logging.info("Reading incoming data...")

# Parse JSON from Kafka value column
raw_messages = df.selectExpr("CAST(value AS STRING)", "timestamp")

# Display schema (good for debugging)
logging.info("Schema of raw messages: %s", raw_messages.printSchema())

# Parse JSON to structured format
messages = raw_messages.select(from_json(col("value"), schema).alias("value_columns"), "timestamp").select("value_columns.*", "timestamp")

logging.info("Parsed messages schema: %s", messages.printSchema())

# Write to console for debugging purposes
query = messages.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Write to MongoDB
messages.writeStream\
        .format("mongodb")\
        .queryName("ToMDB")\
        .option("checkpointLocation", "/tmp/pyspark5/")\
        .option("forceDeleteTempCheckpointLocation", "true")\
        .option('spark.mongodb.connection.uri', 'mongodb://admin:adminpassword@mongo:27017/?authSource=admin')\
        .option('spark.mongodb.database', 'test')\
        .option('spark.mongodb.collection', 'my_collection')\
        .trigger(processingTime="10 seconds")\
        .outputMode("append")\
        .start().awaitTermination()



query.awaitTermination()
