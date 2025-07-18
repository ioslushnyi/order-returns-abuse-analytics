from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, when, to_date, datediff
from pyspark.sql.types import DateType
import argparse
import os

# ! FOR LOCAL EXECUTION, REMOVE FOR DATAPROC !
# os.environ["PYSPARK_PYTHON"] = os.path.abspath(".venv/Scripts/python.exe")
# os.environ["PYSPARK_DRIVER_PYTHON"] = os.path.abspath(".venv/Scripts/python.exe")
# os.environ["HADOOP_HOME"] = "C:/hadoop"

returns_path = "gs://returns-fraud-data/raw/returns.csv"
orders_path = "gs://returns-fraud-data/raw/orders.csv"
output_path = "gs://returns-fraud-data/clean/returns/"

returns_path = "../data/returns.csv"
orders_path = "../data/orders.csv"
output_path = "../data/clean_returns/"

parser = argparse.ArgumentParser()
parser.add_argument("--input_format", choices=["csv", "parquet"], default="csv")
#parser.add_argument("--input_path", required=True, default=returns_path)
parser.add_argument("--output_path", default=output_path)
args = parser.parse_args()

# Init spark
spark = SparkSession.builder \
    .appName("Clean Returns Data") \
    .getOrCreate()

print(f"Spark Version: {spark.version}")

# Load returns data
if args.input_format == "csv":
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(returns_path)
else:
    df = spark.read.parquet(args.input_path)

# Basic cleaning
df_cleaned = df.withColumn(
    "return_reason",
    lower(trim(col("return_reason")))
).withColumn(
    "return_reason",
    when(col("return_reason").isin("", "none", "n/a", "na", "idk", "null"), "unspecified")
    .otherwise(col("return_reason"))
)

# Convert return_date to date type (check if already in date format later)
df_cleaned = df_cleaned.withColumn("return_date", to_date(col("return_date")))

# Join orders to calculate return_days
orders_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(orders_path) \
    .withColumn("order_date", to_date(col("order_date")))

# Calculate return_days
df_joined = df_cleaned.join(orders_df.select("order_id", "order_date"), on="order_id", how="left")
df_joined = df_joined.withColumn("return_days", datediff(col("return_date"), col("order_date")))

# Remove duplicates
df_deduped = df_joined.dropDuplicates(["order_id", "return_date"])

# Output to Parquet in GCS (partitioned by return_reason)
df_deduped.write \
    .mode("overwrite") \
    .partitionBy("return_reason") \
    .parquet(output_path)

print(f"Cleaned returns data written to: {output_path}")
spark.stop()
