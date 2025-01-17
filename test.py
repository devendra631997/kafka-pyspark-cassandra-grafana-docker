import pymongo
mongo_uri = 'mongodb://admin:adminpassword@localhost:27017/'
client = pymongo.MongoClient(mongo_uri)


db = client.test
print(db.name)
print(db.my_collection.find())
records = db.my_collection.find()

# Print each record
for record in records:
    print(record)



# import pyspark
# from pyspark import SparkContext, SparkConf
# from pyspark.sql import SparkSession, SQLContext, functions as F
# from pyspark.sql.functions import *

# # create a spark session
# spark = SparkSession \
# .builder \
# .master("local") \
# .appName("ABC") \
# .config("spark.driver.memory", "15g") \
# .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017/test") \
# .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/test") \
# .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.2') \
# .getOrCreate()
# # read data from mongodb collection "questions" into a dataframe "df"
# df = spark.read \
# .format("mongodb") \
# .option("uri", "mongodb://localhost:27017/test") \
# .option("database", "test") \
# .option("collection", "my_collection") \
# .load()
# df.printSchema()