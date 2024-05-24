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

from pyspark.sql.functions import col, lit

#define the schema
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

#read json using spark dataframe reader
constructor_df = spark.read \
                    .schema(constructors_schema) \
                    .json(f"{raw_folder_path}/{v_file_date}/constructors.json")

#drop url column
constructor_dropped_df = constructor_df.drop(col("url"))

#rename column & add ingestion date column
constructor_with_columns_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                                    .withColumnRenamed("constructorRef", "constructor_ref") \
                                                    .withColumn("data_source", lit(v_data_source)) \
                                                    .withColumn("file_date", lit(v_file_date))

#add ingestion date to the dataframe
constructor_final_df = add_ingestion_date(constructor_with_columns_df)

#display constructor dataframe
display(constructor_final_df)

# COMMAND ----------

#write the data to ADLS as parquet in delta format
constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")
