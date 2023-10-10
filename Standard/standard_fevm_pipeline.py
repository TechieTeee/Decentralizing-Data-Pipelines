import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col
import mysql.connector
from spark_lz4 import lz4_encode, lz4_decode

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the transaction data from FEVM
df = spark.read.format("json").load("https://api.filecoin.io/transactions")

# Filter the data to the last 24 hours
df = df.filter(from_unixtime(col("timestamp")) > "2023-10-10 16:39:51 PST")

# Data quality checks
# Check for empty values
df = df.filter(col("from").isNotNull() and col("to").isNotNull() and col("value").isNotNull())
# Check for duplicate transactions
df = df.distinct()

# Compress the data using Spark-LZ4
df = df.withColumn("compressed_data", lz4_encode(df.toPandas()))

# Create a MySQL connection
conn = mysql.connector.connect(user='mysql_user', password='mysql_password', host='mysql_host', database='mysql_database')

# Create a table in MySQL to store the compressed transaction data
conn.cursor().execute("""CREATE TABLE IF NOT EXISTS transactions_compressed (
    compressed_data BLOB NOT NULL
)""")

# Load the compressed transaction data into MySQL
df.write.format("jdbc").option("url", "jdbc:mysql://mysql_host:3306/mysql_database").option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "transactions_compressed").mode("append").save()

# Close the MySQL connection
conn.close()

# Stop the SparkSession
spark.stop()