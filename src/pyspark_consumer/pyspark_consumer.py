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

stock_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .option("startingOffsets", "latest") \
        .load()

logging.info("Reading incoming data...")

# simply converting the 'value' column from json to a string and keeping the 'timestamp' column as is
stock_df1 = stock_df.selectExpr("CAST(value AS STRING)", "timestamp")
logging.info("stock_df1", stock_df1)

# parsing the 'value' column, which is a json string, 
# and fitting the extracted columns to the schema we defined. 
# finally, we gave the name "value_columns" to this column which contains our required columns 
stock_df2 = stock_df1\
        .select(from_json(col("value"), schema).alias("value_columns"), "timestamp")

# selecting all columns in the "value_columns" column
stock_df3 = stock_df2.select("value_columns.*", "timestamp")

# performing a simple transformation that subtracts the "Open" and "Close" columns
# and creates a new column called "PriceDiff"
# stock_df4 = stock_df3.withColumn("PriceDiff", col("Close") - col("Open"))

# this is the df we want to send
# and it contains only 3 columns
# output_df = stock_df4.select("Open", "Close", "PriceDiff")


# Get the current working directory
current_directory = os.getcwd()

# Set the checkpoint location to the current directory
checkpoint_location = os.path.join(current_directory, "checkpoint")

# below, we're taking all 3 columns, 
# converting them to a json format, 
# and giving the resulting column the name "value"
# oputputMode allows us to only send df as new values are added in it
# checkpointLocation is a good practice to include in case of fault tracking
stock_write_stream = stock_df3.selectExpr("to_json(struct(*)) AS value") \
                              .writeStream \
                              .format("kafka") \
                              .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
                              .option("topic", KAFKA_OUTPUT_TOPIC_NAME_CONS) \
                              .trigger(processingTime='1 seconds') \
                              .outputMode("update") \
                              .option("checkpointLocation", checkpoint_location) \
                              .start()

stock_write_stream.awaitTermination()
