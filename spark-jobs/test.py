from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("../data/clean_returns")  # or your path

df.printSchema()
df.show(5)
