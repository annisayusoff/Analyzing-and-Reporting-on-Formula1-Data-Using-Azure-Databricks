# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, to_timestamp, concat, lit

#define the schema
races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)    
])

#read csv using spark dataframe reader
races_df = spark.read \
            .option("header", True) \
            .schema(races_schema) \
            .csv(f"{raw_folder_path}/{v_file_date}/races.csv")

#combine date & time column & add ingestion date column
races_with_timestamp_df = races_df \
    .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

#add ingestion date to the dataframe
races_ingestion_date_df = add_ingestion_date(races_with_timestamp_df)

#select & rename columns 
races_final_df = races_ingestion_date_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), col("circuitId").alias("circuit_id"), col("name"), col("ingestion_date"), col("race_timestamp"), col("data_source"), col("file_date"))

#display races dataframe
display(races_final_df)

# COMMAND ----------

#write the data to ADLS as parquet
# circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/races")
# circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")

races_final_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")
