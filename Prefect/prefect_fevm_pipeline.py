import prefect
from prefect import task, Flow
from prefect.tasks.data_engineering import SparkTask, MySQLTask

# Define a task to read the transaction data from FEVM
@task
def read_fevm_transactions():
    """Reads the transaction data from FEVM and returns a Spark DataFrame."""

    spark = prefect.context.get("spark")
    df = spark.read.format("json").load("https://api.filecoin.io/transactions")
    return df

# Define a task to filter the data to the last 24 hours
@task
def filter_transactions(df):
    """Filters the transaction data to the last 24 hours and returns a Spark DataFrame."""

    from pyspark.sql.functions import from_unixtime, col

    return df.filter(from_unixtime(col("timestamp")) > "2023-10-10 16:39:51 PST")

# Define a task to perform data quality checks
@task
def perform_data_quality_checks(df):
    """Performs data quality checks on the transaction data and returns a Spark DataFrame."""

    # Check for empty values
    df = df.filter(col("from").isNotNull() and col("to").isNotNull() and col("value").isNotNull())
    # Check for duplicate transactions
    df = df.distinct()
    return df

# Define a task to compress the data using Spark-LZ4
@task
def compress_data(df):
    """Compresses the transaction data using Spark-LZ4 and returns a Spark DataFrame."""

    from spark_lz4 import lz4_encode

    df = df.withColumn("compressed_data", lz4_encode(df.toPandas()))
    return df

# Define a task to load the compressed transaction data into MySQL
@task
def load_compressed_data_into_mysql(df):
    """Loads the compressed transaction data into MySQL."""

    mysql_conn = prefect.context.get("mysql_conn")

    df.write.format("jdbc").option("url", mysql_conn.url).option("driver", mysql_conn.driver).option("dbtable", "transactions_compressed").mode("append").save()

# Create a Flow to orchestrate the tasks
with Flow("Extract, Load, and Transform FEVM Transactions") as flow:
    # Read the transaction data from FEVM
    df = read_fevm_transactions()

    # Filter the data to the last 24 hours
    df = filter_transactions(df)

    # Perform data quality checks
    df = perform_data_quality_checks(df)

    # Compress the data using Spark-LZ4
    df = compress_data(df)

    # Load the compressed transaction data into MySQL
    load_compressed_data_into_mysql(df)

# Run the Flow
flow.run()
