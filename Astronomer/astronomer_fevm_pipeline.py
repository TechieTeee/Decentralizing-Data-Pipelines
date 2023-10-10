import airflow
from airflow import DAG
from airflow.operators import python_operator, mysql_operator, spark_operator

# Define a Python operator to read the transaction data from FEVM
def read_fevm_transactions(**kwargs):
    """Reads the transaction data from FEVM and returns a Spark DataFrame."""

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    df = spark.read.format("json").load("https://api.filecoin.io/transactions")
    return df

# Define a Python operator to filter the data to the last 24 hours
def filter_transactions(df, **kwargs):
    """Filters the transaction data to the last 24 hours and returns a Spark DataFrame."""

    from pyspark.sql.functions import from_unixtime, col

    return df.filter(from_unixtime(col("timestamp")) > "2023-10-10 16:39:51 PST")

# Define a Python operator to perform data quality checks
def perform_data_quality_checks(df, **kwargs):
    """Performs data quality checks on the transaction data and returns a Spark DataFrame."""

    # Check for empty values
    df = df.filter(col("from").isNotNull() and col("to").isNotNull() and col("value").isNotNull())
    # Check for duplicate transactions
    df = df.distinct()
    return df

# Define a Python operator to compress the data using Spark-LZ4
def compress_data(df, **kwargs):
    """Compresses the transaction data using Spark-LZ4 and returns a Spark DataFrame."""

    from spark_lz4 import lz4_encode

    df = df.withColumn("compressed_data", lz4_encode(df.toPandas()))
    return df

# Define a MySQL operator to load the compressed transaction data into MySQL
def load_compressed_data_into_mysql(df, **kwargs):
    """Loads the compressed transaction data into MySQL."""

    query = """INSERT INTO transactions_compressed (compressed_data) VALUES (:compressed_data)"""

    mysql_conn = mysql_operator.MySQLHook(conn_id='mysql_conn')

    for row in df.collect():
        mysql_conn.run(query, parameters={'compressed_data': row['compressed_data']})

# Create a DAG to orchestrate the tasks
default_args = {
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 1
}

dag = DAG('extract_load_and_transform_fevm_transactions', default_args=default_args, schedule_interval='@daily')

# Read the transaction data from FEVM
read_fevm_transactions_task = python_operator.PythonOperator(
    task_id='read_fevm_transactions',
    python_callable=read_fevm_transactions,
    dag=dag
)

# Filter the data to the last 24 hours
filter_transactions_task = python_operator.PythonOperator(
    task_id='filter_transactions',
    python_callable=filter_transactions,
    dag=dag
)

# Perform data quality checks
perform_data_quality_checks_task = python_operator.PythonOperator(
    task_id='perform_data_quality_checks',
    python_callable=perform_data_quality_checks,
    dag=dag
)

# Compress the data using Spark-LZ4
compress_data_task = python_operator.PythonOperator(
    task_id='compress_data',
    python_callable=compress_data,
    dag=dag
)

# Load the compressed transaction data into MySQL
load_compressed_data_into_mysql_task = python_operator.PythonOperator(
    task_id='load_compressed_data_into_mysql',
    python_callable=load_compressed_data_into_mysql,
    dag=dag
)

# Set the task dependencies
read_fevm_transactions_task >> filter_transactions_task >> perform_data_quality_checks_task >> compress_data_task >> load_compressed_data_into_mysql_task
