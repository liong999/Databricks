# Databricks notebook source
# Import necessary libraries
from pyspark.sql import SparkSession

# Create a list to declare jdbc_url, scope_name, PostgreSQL_tbl_vw and delta_tbl
jdbc_url = [
    "jdbc:postgresql://teds-asp-db.c2zymjdjsukl.ap-southeast-1.rds.amazonaws.com:5432/TEDS_ASP",
    "jdbc:postgresql://odoo-db-9619.cluster-c2zymjdjsukl.ap-southeast-1.rds.amazonaws.com:5432/teds_db"]
scope_name = ["PostgreSQL-DM","PostgreSQL-ASP","PostgreSQL-TDM"]
PostgreSQL_tbl_vw = ["cdb_md5_history","mv_cdb_md5","mv_cdb_md5"]
delta_tbl = ["honda.propensity.bronze_dm_cdb_md5_history","honda.propensity.bronze_asp_mv_cdb_md5","honda.propensity.bronze_tdm_mv_cdb_md5"]

# Use this list index to control which source system to connect and extract data
# 0 = PostgreSQL Datamart (Delman), 1 = PostgreSQL ASP, 2 = PostgreSQL TDM
list_index = [1, 2]

# Create a Spark session
spark = SparkSession.builder.appName("postgresql").getOrCreate()

# Loop through the list index to connect and extract data before loading into the delta tables
for i in list_index:
    # Get credentials from Databricks Secrets
    username = dbutils.secrets.get(scope=scope_name[i], key="username")
    password = dbutils.secrets.get(scope=scope_name[i], key="password")

    # Connection properties
    connection_properties = {
        "user": username,
        "password": password,
        "driver": "org.postgresql.Driver"
    }

    # Read data from PostgreSQL into a dataframe with partitions for parallelism and performance
    df = spark.read.jdbc(url=jdbc_url[i], table=PostgreSQL_tbl_vw[i], properties=connection_properties, numPartitions=100)

    # Merge the schema of the DataFrame with the existing Delta Lake table and ignore because mv_cdb_md5 will take rolling last 1 month data
    # In this mode, if the destination already exists, the write operation is simply ignored, and no changes are made. If the destination doesn't exist, a new one is created.
    df.write.option("mergeSchema", "true").mode("append").saveAsTable(delta_tbl[i])

# COMMAND ----------

# Import necessary libraries
import boto3
import pandas as pd
import io
import warnings
from pyspark.sql import SparkSession

# Suppress the FutureWarnings
warnings.filterwarnings("ignore", category=FutureWarning)

# Create a list to scope_name, s3 bucket and delta_tbl
scope_name = "S3Bucket"
bucket_name = "databrick-workspace-tunashonda"
bucket_file_path = ["data/master-data/bronze_master_data.csv","data/master-data/bronze_customer_group.csv"]
delta_tbl = ["honda.propensity.bronze_master_data","honda.propensity.bronze_customer_group"]

# Use this list index to control which csv file to extract data from S3 bucket
# 0 = Master Data csv, 1 = Customer Group csv
list_index = [0, 1]

# Get credentials from Databricks Secrets
username = dbutils.secrets.get(scope=scope_name, key="username")
password = dbutils.secrets.get(scope=scope_name, key="password")

# Create an S3 client
s3 = boto3.client('s3', aws_access_key_id=username, aws_secret_access_key=password)

# Create a Spark session
spark = SparkSession.builder.appName("s3").getOrCreate()

# Loop through the list index to connect and extract data before loading into the delta tables
for i in list_index:
    # Read the CSV file from S3
    try:
        response = s3.get_object(Bucket=bucket_name, Key=bucket_file_path[i])
        csv_data = response['Body'].read().decode('utf-8')
    except Exception as e:
        print(f"Error reading CSV file from S3: {str(e)}")
        csv_data = None

    # Check if CSV data was successfully retrieved
    if csv_data:
        # Create a DataFrame from the CSV data
        pd_df = pd.read_csv(io.StringIO(csv_data), sep=';', header=0)

        # Create a spark DataFrame from the list of row objects
        spark_df = spark.createDataFrame(pd_df)

        # Merge the schema of the DataFrame with the existing Delta Lake table
        # In this mode, if the destination already exists, the write operation is simply ignored, and no changes are made. If the destination doesn't exist, a new one is created.
        spark_df.write.option("mergeSchema", "true").mode("overwrite").saveAsTable(delta_tbl[i])
    else:
        print("CSV data was not retrieved from S3.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM honda.propensity.bronze_dm_cdb_md5_history WHERE data_source = 'ASP' ORDER BY date_order DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM honda.propensity.bronze_dm_cdb_md5_history WHERE data_source = 'TDM' ORDER BY date_order DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM honda.propensity.bronze_dm_cdb_md5_history

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM honda.propensity.bronze_asp_mv_cdb_md5 ORDER BY date_order DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM honda.propensity.bronze_asp_mv_cdb_md5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM honda.propensity.bronze_tdm_mv_cdb_md5 ORDER BY date_order DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM honda.propensity.bronze_tdm_mv_cdb_md5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM honda.propensity.bronze_master_data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM honda.propensity.bronze_master_data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM honda.propensity.bronze_customer_group

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM honda.propensity.bronze_customer_group
