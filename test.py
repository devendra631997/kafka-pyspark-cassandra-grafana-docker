# import pymongo
# mongo_uri = 'mongodb://admin:adminpassword@localhost:27017/'
# client = pymongo.MongoClient(mongo_uri)


# db = client.test
# print(db.name)
# print(db.my_collection.find())
# records = db.my_collection.find()

# # Print each record
# for record in records:
#     print(record)

# db.my_collection.delete_many({})


######################################### FOR CASANDRA
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Define your Cassandra cluster settings
cassandra_nodes = ['127.0.0.1']  # IP addresses of your Cassandra nodes
keyspace = 'interaction_data'  # Replace with your keyspace name
    #   - CASSANDRA_HOST=cassandra  # Use service name 'cassandra' here
    #   - CASSANDRA_KEYSPACE=interaction_data
    #   - CASSANDRA_TABLE=interaction_data
# Optionally, set up authentication if required (replace 'username' and 'password')
auth_provider = PlainTextAuthProvider(username='your_username', password='your_password')

# Create a cluster connection
cluster = Cluster(cassandra_nodes)

# Connect to the cluster
session = cluster.connect(keyspace)

# Perform a sample query
try:
    # Sample query to check keyspaces
    rows = session.execute("SELECT * FROM interaction_data.interaction_data")
    for row in rows:
        print(row)
except Exception as e:
    print(f"Error executing query: {e}")

# Close the session and connection
session.shutdown()
cluster.shutdown()
 