# Databricks notebook source
# Import necessary libraries
from pyspark.sql import SparkSession

# Create a list to declare jdbc_url, scope_name, and PostgreSQL_tbl_vw
jdbc_url = [
    "jdbc:postgresql://teds-asp-db.c2zymjdjsukl.ap-southeast-1.rds.amazonaws.com:5432/TEDS_ASP",
    "jdbc:postgresql://odoo-db-9619.cluster-c2zymjdjsukl.ap-southeast-1.rds.amazonaws.com:5432/teds_db"]
scope_name = ["PostgreSQL-ASP", "PostgreSQL-TDM"]
PostgreSQL_tbl_vw = ["cron_data", "cron_data"]

# List to store the dataframes
dataframes_list = []

# Use this list index to control which source system to connect and extract data
# 0 = PostgreSQL Datamart (Delman), 1 = PostgreSQL ASP, 2 = PostgreSQL TDM
list_index = [1, 2]

# Create a Spark session
spark = SparkSession.builder.appName("postgresql").getOrCreate()

# Loop through the list index to connect and extract data
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

    # Read data from PostgreSQL into a dataframe
    df = spark.read.jdbc(
        url=jdbc_url[i], 
        table=PostgreSQL_tbl_vw[i], 
        properties=connection_properties, 
        numPartitions=100
    )
    
    # Add the dataframe to our list
    dataframes_list.append(df)

# Now dataframes_list contains all your dataframes
# You can access them like this:
# df_asp = dataframes_list[0]  # Data from PostgreSQL-ASP
# df_tdm = dataframes_list[1]  # Data from PostgreSQL-TDM

# If you want to combine all data into a single dataframe:
if dataframes_list:
    combined_df = dataframes_list[0]
    for df in dataframes_list[1:]:
        combined_df = combined_df.union(df)
    # Now combined_df contains all data from both sources

print(str(combined_df.collect()))