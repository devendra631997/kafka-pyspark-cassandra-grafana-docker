from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
import os
import logging

# Environment variables
KAFKA_TOPIC_NAME_CONS = os.environ.get('INPUT_TOPIC')
KAFKA_OUTPUT_TOPIC_NAME_CONS = os.environ.get('OUTPUT_TOPIC')
KAFKA_BOOTSTRAP_SERVERS_CONS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS')
CASSANDRA_HOST = os.environ.get('CASSANDRA_HOST', 'cassandra')  # Default to "cassandra" host
CASSANDRA_KEYSPACE = os.environ.get('CASSANDRA_KEYSPACE', 'interaction_data')
CASSANDRA_TABLE = os.environ.get('CASSANDRA_TABLE', 'interaction_data')

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define schema for incoming messages
schema = (StructType()
          .add("interaction_type", StringType())
          .add("timestamp", TimestampType())
          .add("user_id", DoubleType())
          .add("item_id", DoubleType()))


# Function to initialize Spark session
def create_spark_session() -> SparkSession:
    """Initialize and return a Spark session."""
    spark = (SparkSession.builder.appName("streaming_interaction")
             .config('spark.jars.packages',
                     'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,'
                     'com.datastax.spark:spark-cassandra-connector_2.12:3.0.0')
             .config('spark.sql.streaming.checkpointLocation', '/tmp/pyspark5/')
             .config('spark.sql.shuffle.partitions', 8)
             .config("spark.cassandra.connection.host", CASSANDRA_HOST)
             .config("spark.cassandra.connection.port", "9042")
             .getOrCreate())

    spark.sparkContext.setLogLevel("INFO")
    return spark


# Function to read data from Kafka
def read_from_kafka(spark: SparkSession):
    """Read streaming data from Kafka."""
    df = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS)
          .option("subscribe", KAFKA_TOPIC_NAME_CONS)
          .option("startingOffsets", "latest")
          .option("failOnDataLoss", "false")
          .load())

    logger.info(f"Started reading from Kafka topic: {KAFKA_TOPIC_NAME_CONS}")
    return df


# Function to parse raw Kafka messages
def parse_kafka_messages(df, schema):
    """Parse raw Kafka messages into structured format."""
    raw_messages = df.selectExpr("CAST(value AS STRING)")
    messages = (raw_messages
                .select(from_json(col("value"), schema).alias("value_columns"))
                .select("value_columns.*"))

    logger.info("Parsed messages schema:")
    messages.printSchema()
    return messages


def aggregate_user_interactions(df):
    """Aggregates total interactions per user."""
    return (df.groupBy("user_id")
            .count()
            .withColumnRenamed("count", "total_interactions")
            .withColumn("average_interactions", col("total_interactions") / 1))  # Adjust as necessary for your logic

def aggregate_item_interactions(df):
    """Aggregates max, min, and total interactions per item."""
    return (df.groupBy("item_id")
            .count()
            .withColumnRenamed("count", "total_interactions")
            .withColumn("average_interactions", col("total_interactions") / 1))


# Function to write data to Cassandra
def write_to_cassandra(df, keyspace=CASSANDRA_KEYSPACE, table=CASSANDRA_TABLE, mode: str = "append"):
    """Write processed data to Cassandra."""
    # Log the keyspace and table to check their values
    logger.info(f"Writing to Cassandra: Keyspace={keyspace}, Table={table}, Mode={mode}")

    # Ensure that the keyspace and table are passed explicitly in the options
    query = (df.writeStream
             .format("org.apache.spark.sql.cassandra")
             .option("keyspace", keyspace)
             .option("table", table)
             .outputMode(mode)  # Ensure mode is either "append" or "complete"
             .option("confirm.truncate", "true")
             .start())

    logger.info("Writing stream to Cassandra...")
    return query


# Main function to orchestrate the process
def main():
    """Main function to run the streaming job."""
    spark = create_spark_session()  # Step 1: Create Spark session
    df = read_from_kafka(spark)  # Step 2: Read from Kafka
    messages = parse_kafka_messages(df, schema)  # Step 3: Parse messages

    # Aggregation for user interactions
    user_agg_df = aggregate_user_interactions(messages)
    user_agg_df_query = write_to_cassandra(user_agg_df, table='user_aggregates', mode='complete')

    # Aggregation for item interactions
    item_agg_df = aggregate_item_interactions(messages)
    item_agg_df_query = write_to_cassandra(item_agg_df, table='item_aggregates', mode='complete')

    # Write raw messages in append mode (since it's streaming data)
    raw_messages_query = write_to_cassandra(messages, mode="append")

    # Handle graceful shutdown and errors
    try:
        logger.info("Starting query processing...")
        # Await termination for all the queries in parallel
        user_agg_df_query.awaitTermination()
        item_agg_df_query.awaitTermination()
        raw_messages_query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Streaming job interrupted.")
        # Stop all queries gracefully
        user_agg_df_query.stop()
        item_agg_df_query.stop()
        raw_messages_query.stop()

    except Exception as e:
        logger.error(f"Error in streaming job: {e}")
        # Stop all queries in case of error
        user_agg_df_query.stop()
        item_agg_df_query.stop()
        raw_messages_query.stop()


if __name__ == "__main__":
    main()
