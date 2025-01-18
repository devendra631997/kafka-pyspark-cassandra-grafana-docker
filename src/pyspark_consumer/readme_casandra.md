### PySpark Consumer Service with casandra

The **PySpark consumer service** reads interaction data from Kafka, processes it, performs aggregation on user and item interactions, and writes the results to Cassandra. It runs as a Spark streaming job, continuously processing data in real-time.

#### How It Works

1. **Kafka**:
   The service reads interaction data from the Kafka topic specified by the `INPUT_TOPIC` environment variable (`interaction` by default). It uses the Kafka connector for PySpark to consume the data.

2. **Data Aggregation**:
   The service aggregates the data in two ways:
   - **User Aggregation**: Counts the number of interactions per user.
   - **Item Aggregation**: Counts the number of interactions per item.

3. **Cassandra**:
   The aggregated results are written to Cassandra under the keyspace and table specified in the environment variables (`CASSANDRA_KEYSPACE`, `CASSANDRA_TABLE`). The raw messages are also written to Cassandra.

#### Running the PySpark Consumer

To start the PySpark consumer (along with the rest of the services):

1. **Build and Start the Services**:

   From the root of the repository, run:

   ```bash
   docker-compose up --build
   ```

   This will build the images and start the Kafka, Cassandra, and PySpark consumer services, along with the other necessary services like Zookeeper and the producer.

2. **View Logs**:

   To view the logs and see the data being processed:

   ```bash
   docker-compose logs -f pyspark_consumer
   ```

3. **Stop the Services**:

   To stop all containers:

   ```bash
   docker-compose down
   ```

#### Example Aggregation Result

After the consumer has been running for some time, it will process interactions and store the aggregated results in Cassandra. Here’s an example of the aggregation data stored in Cassandra:

- **User Aggregates** (stored in the `user_aggregates` table):
  ```json
  { "user_id": 11101, "total_interactions": 10, "average_interactions": 5.0 }
  ```

- **Item Aggregates** (stored in the `item_aggregates` table):
  ```json
  { "item_id": 5, "total_interactions": 50, "average_interactions": 25.0 }
  ```
