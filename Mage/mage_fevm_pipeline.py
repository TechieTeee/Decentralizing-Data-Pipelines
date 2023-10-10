import mage

# Define a Mage pipeline
pipeline = mage.Pipeline(name="Extract, Load, and Transform FEVM Transactions")

# Define a stage to read the transaction data from FEVM
stage1 = mage.Stage(name="Read FeVM Transactions")
stage1.task(
    name="Read FeVM Transactions Task",
    command="python read_fevm_transactions.py",
    output="transactions.json",
)

# Define a stage to filter the data to the last 24 hours
stage2 = mage.Stage(name="Filter Transactions")
stage2.task(
    name="Filter Transactions Task",
    command="python filter_transactions.py transactions.json transactions_filtered.json",
    input="transactions.json",
    output="transactions_filtered.json",
)

# Define a stage to perform data quality checks
stage3 = mage.Stage(name="Perform Data Quality Checks")
stage3.task(
    name="Perform Data Quality Checks Task",
    command="python perform_data_quality_checks.py transactions_filtered.json transactions_quality_checked.json",
    input="transactions_filtered.json",
    output="transactions_quality_checked.json",
)

# Define a stage to compress the data using Spark-LZ4
stage4 = mage.Stage(name="Compress Data")
stage4.task(
    name="Compress Data Task",
    command="python compress_data.py transactions_quality_checked.json transactions_compressed.json",
    input="transactions_quality_checked.json",
    output="transactions_compressed.json",
)

# Define a stage to load the compressed transaction data into MySQL
stage5 = mage.Stage(name="Load Compressed Data into MySQL")
stage5.task(
    name="Load Compressed Data into MySQL Task",
    command="python load_compressed_data_into_mysql.py transactions_compressed.json",
    input="transactions_compressed.json",
)

# Define the pipeline dependencies
pipeline.add_stage(stage1)
pipeline.add_stage(stage2)
stage1 >> stage2
pipeline.add_stage(stage3)
stage2 >> stage3
pipeline.add_stage(stage4)
stage3 >> stage4
pipeline.add_stage(stage5)
stage4 >> stage5

# Run the pipeline
pipeline.run()