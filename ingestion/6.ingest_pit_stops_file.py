# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import lit

#define the schema
pit_stops_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
])

#read json using spark dataframe reader
pit_stops_df = spark.read \
                .schema(pit_stops_schema) \
                .option("multiLine", True) \
                .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

#rename & add ingestion date column
pit_stops_with_column_df = pit_stops_df.withColumnRenamed("raceId", "race_id") \
                                       .withColumnRenamed("driverId", "driver_id") \
                                       .withColumn("data_source", lit(v_data_source)) \
                                       .withColumn("file_date", lit(v_file_date))
                        
#add ingestion date to the dataframe
pit_stops_final_df = add_ingestion_date(pit_stops_with_column_df)

#display pitstops dataframe
display(pit_stops_final_df)

# COMMAND ----------

#write the data to ADLS as parquet in delta format
merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(pit_stops_final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")
