# Databricks notebook source
import json

# COMMAND ----------

catalog_name = 'sandbox'

# COMMAND ----------

# config_raw_df = spark.sql(f"select * except(rec_created_date) from {catalog_name}.config.control_raw")
config_raw_df = spark.sql(f"select * except(rec_created_date) from {catalog_name}.dbx_demo_config.optiva_config_raw_tables")
config_raw_df_pd = config_raw_df.toPandas()
task_list = config_raw_df_pd.to_dict(orient='records')
task_json_list = [eval(json.dumps(d)) for d in task_list]

# COMMAND ----------

config_raw_df.display()

# COMMAND ----------

task_json_list

# COMMAND ----------

# Creating Job task Values
dbutils.jobs.taskValues.set(key = 'raw_load', value = task_json_list)