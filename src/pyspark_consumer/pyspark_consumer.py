from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
import os
import logging

# Environment variables
KAFKA_TOPIC_NAME_CONS = os.environ.get('INPUT_TOPIC')
KAFKA_OUTPUT_TOPIC_NAME_CONS = os.environ.get('OUTPUT_TOPIC')
KAFKA_BOOTSTRAP_SERVERS_CONS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS')
MONGO_USER = os.environ.get('MONGO_INITDB_ROOT_USERNAME')
MONGO_PASS = os.environ.get('MONGO_INITDB_ROOT_PASSWORD')
MONGO_DATA_BASE = os.environ.get('MONGO_DATA_BASE', 'test')
MONGO_COLLECTION = os.environ.get('MONGO_COLLECTION', 'my_collection')


# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define schema for incoming messages
schema = StructType().add("interaction_type", StringType()) \
                     .add("timestamp", TimestampType()) \
                     .add("user_id", DoubleType()) \
                     .add("item_id", DoubleType())

# Function to initialize Spark session
def create_spark_session() -> SparkSession:
    """Initialize and return a Spark session."""
    spark = SparkSession.builder \
        .appName("streaming_interaction") \
        .config('spark.jars.packages', 
                'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,' 
                'org.mongodb.spark:mongo-spark-connector_2.12:10.1.1') \
        .config('spark.sql.streaming.checkpointLocation', '/tmp/pyspark5/') \
        .config('spark.sql.shuffle.partitions', 8) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("INFO")
    return spark

# Function to read data from Kafka
def read_from_kafka(spark: SparkSession):
    """Read streaming data from Kafka."""
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    logger.info("Started reading from Kafka topic: %s", KAFKA_TOPIC_NAME_CONS)
    return df

# Function to parse raw Kafka messages
def parse_kafka_messages(df, schema):
    """Parse raw Kafka messages into structured format."""
    raw_messages = df.selectExpr("CAST(value AS STRING)", "timestamp")
    messages = raw_messages \
        .select(from_json(col("value"), schema).alias("value_columns"), "timestamp") \
        .select("value_columns.*", "timestamp")  # No cache here, streaming only.
    logger.info("Parsed messages schema: %s", messages.printSchema())
    return messages

# Function to write data to MongoDB
def write_to_mongodb(df):
    """Write processed data to MongoDB."""
    query = df.writeStream \
        .format("mongodb") \
        .queryName("ToMDB") \
        .option("checkpointLocation", "/tmp/pyspark5/") \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .option('spark.mongodb.connection.uri', f'mongodb://{MONGO_USER}:{MONGO_PASS}@mongo:27017/?authSource=admin') \
        .option('spark.mongodb.database', MONGO_DATA_BASE) \
        .option('spark.mongodb.collection', MONGO_COLLECTION) \
        .trigger(processingTime="5 seconds") \
        .outputMode("append") \
        .start()
    logger.info("Writing stream to MongoDB...")
    return query

# Main function to orchestrate the process
def main():
    """Main function to run the streaming job."""
    spark = create_spark_session()  # Step 1: Create Spark session
    df = read_from_kafka(spark)  # Step 2: Read from Kafka
    messages = parse_kafka_messages(df, schema)  # Step 3: Parse messages
    query = write_to_mongodb(messages)  # Step 4: Write to MongoDB
    
    # Wait for the query to terminate
    query.awaitTermination()

if __name__ == "__main__":
    main()
