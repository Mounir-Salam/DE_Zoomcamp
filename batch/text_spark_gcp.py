from pyspark.sql import SparkSession
import pyspark

# Define the GCS Connector version (Hadoop 3 compatible)
GCS_CONNECTOR_VERSION = "hadoop3-2.2.22" # Check Maven for the absolute latest

spark = SparkSession.builder \
    .appName("GCP-Connect") \
    .config("spark.jars.packages", f"com.google.cloud.bigdataoss:gcs-connector:{GCS_CONNECTOR_VERSION}") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/home/mounir_salam/python_projects/DE_Zoomcamp/pipeline/keys/de-zoomcamp-key.json") \
    .getOrCreate()

# Test the connection
df = spark.read.parquet("gs://de-zoomcamp-484617-function_bucket/green_tripdata_2019-01.parquet")
df.show(5)

