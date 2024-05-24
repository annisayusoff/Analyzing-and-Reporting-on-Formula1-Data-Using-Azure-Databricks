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
lap_times_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
])

#read multiple csv using spark dataframe reader
lap_times_df = spark.read \
                .schema(lap_times_schema) \
                .csv(f"{raw_folder_path}/{v_file_date}/lap_times")

#rename & add ingestion date column
lap_times_with_column_df = lap_times_df.withColumnRenamed("raceId", "race_id") \
                                       .withColumnRenamed("driverId", "driver_id") \
                                       .withColumn("data_source", lit(v_data_source)) \
                                       .withColumn("file_date", lit(v_file_date))
 
#add ingestion date to the dataframe
lap_times_final_df = add_ingestion_date(lap_times_with_column_df)

#display lap times dataframe
display(lap_times_final_df)

# COMMAND ----------

#write the data to ADLS as parquet in delta format
merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(lap_times_final_df, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")
