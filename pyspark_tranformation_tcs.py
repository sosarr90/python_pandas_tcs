import os
# Configure PySpark to use the Python interpreter from the virtual environment
# This ensures that both the driver and the executors use the same Python version
os.environ["PYSPARK_PYTHON"] = "C:\\Users\\sarrs\\pyspark_transformation_tcs\\venv\\Scripts\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Users\\sarrs\\pyspark_transformation_tcs\\venv\\Scripts\\python.exe"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    concat, col, lpad, lit, sum as _sum, max as _max,
    to_timestamp, date_format, regexp_extract
)
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# 1. Start Spark session
spark = SparkSession.builder \
    .appName("TransactionDataProcessing") \
    .getOrCreate()

# Set Spark log level to ERROR to reduce verbosity
spark.sparkContext.setLogLevel("ERROR")

# 2. Sample input data
data = [
    (2025, 1, 15, 15, 59, "AA", "1a"),
    (2025, 2, 14, 23, 55, "B", "2"),
    (2021, 10, 13, 22, 55, "AA", "1"),
    (2023, 11, 12, 8, 50, "AA", "2"),
    (2025, 3, 11, 7, 45, "AA", "1"),
    (1990, 4, 10, 2, 45, "B", "2"),
    (2001, 4, 9, 11, 45, "B", "1"),
    (1990, 4, 10, 2, 45, "B", "2"),
    (2021, 10, 13, 22, 55, "AA", "1")
]

# 3. Define schema
schema = StructType([
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("hour", IntegerType(), True),
    StructField("seconds", IntegerType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("amount", StringType(), True),  # Amount as string because of hex values like "1a"
])

# 4. Create DataFrame
df = spark.createDataFrame(data, schema)

# 5. Clean and cast amount column (extract digits, cast to int)
df_cleaned = df.withColumn("amount", regexp_extract("amount", r"\d+", 0).cast("int"))

# 6. Create timestamp column with proper concatenation and standard format parsing
df_with_ts = df_cleaned.withColumn(
    "timestamp",
    to_timestamp(
        concat(
            col("year").cast("string"), lit("-"),
            lpad(col("month"), 2, "0"), lit("-"),
            lpad(col("day"), 2, "0"), lit(" "),
            lpad(col("hour"), 2, "0"), lit(":"),
            lit("00"), lit(":"),
            lpad(col("seconds"), 2, "0")
        ),
        "yyyy-MM-dd HH:mm:ss"
    )
)

# 7. Calculate total amount per transaction_id
amount_df = df_with_ts.groupBy("transaction_id").agg(_sum("amount").alias("total_amount"))

# 8. Find most recent transaction date per transaction_id
recent_df = df_with_ts.groupBy("transaction_id") \
    .agg(_max("timestamp").alias("most_recent_transaction_date"))

# 9. Join total amount and recent transaction date dataframes
final_df = amount_df.join(recent_df, on="transaction_id")

# 10. Format most recent transaction date as [mm-YYYY-dd hour:minute:second]
final_df_formatted = final_df.withColumn(
    "most_recent_transaction_date",
    date_format("most_recent_transaction_date", "MM-yyyy-dd HH:mm:ss")
)

# 11. Show final output dataframe
final_df_formatted.select(
    "transaction_id", "total_amount", "most_recent_transaction_date"
).show(truncate=False)

# Stop the Spark session
spark.stop()
