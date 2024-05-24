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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import lit

#define the schema
qualifying_schema = StructType(fields=[
    StructField("qualifyingId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True)
])

#read multiple csv using spark dataframe reader
qualifying_df = spark.read \
                .schema(qualifying_schema) \
                .option("multiLine", True) \
                .json(f"{raw_folder_path}/{v_file_date}/qualifying")

#rename & add ingestion date column
qualifying_with_columns_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
                                          .withColumnRenamed("raceId", "race_id") \
                                          .withColumnRenamed("driverId", "driver_id") \
                                          .withColumnRenamed("constructorId", "constructor_id") \
                                          .withColumn("data_source", lit(v_data_source)) \
                                          .withColumn("file_date", lit(v_file_date))

#add ingestion date to the dataframe
qualifying_final_df = add_ingestion_date(qualifying_with_columns_df)

#display lap times dataframe
display(qualifying_final_df)

# COMMAND ----------

#write the data to ADLS as parquet in delta format
merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(qualifying_final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")
