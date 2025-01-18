### MongoDB Consumer Service

The **MongoDB consumer service** reads interaction data from Kafka, processes it in real time, and writes the processed data to a MongoDB collection.

#### How It Works

1. **Kafka Consumption**:
   - The consumer reads interaction data from the Kafka topic specified by the `INPUT_TOPIC` environment variable (`interaction`).
   - The data is processed in real time using Spark Streaming.

2. **MongoDB Writing**:
   - The processed data is written to a MongoDB collection defined by the `MONGO_COLLECTION` environment variable (`interaction_collection`).

#### Running the MongoDB Consumer

1. **Build and Start Services**:

   Run the following to build and start all the services:

   ```bash
   docker-compose up --build
   ```

2. **View Logs**:

   To view logs from the PySpark Mongo consumer:

   ```bash
   docker-compose logs -f pyspark_mongo_consumer
   ```

3. **Stop Services**:

   To stop all services:

   ```bash
   docker-compose down
   ```

#### Example MongoDB Document

The consumer writes the following data to MongoDB:

```json
{
  "interaction_type": "click",
  "timestamp": "2025-01-18T12:30:45.000Z",
  "user_id": 11101,
  "item_id": 3
}
```
