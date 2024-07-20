# Databricks notebook source
import time
import argparse
from pyspark.sql import functions as F
import sys
import os

sys.path.append("../python")
from sparkbuilder.utils.config_handler import ConfigHandler
from sparkbuilder.builder.engine import PipelineBuilder
import re

# COMMAND ----------

# Local pipeline functions

def add_timestamp_cols(input_df):
    input_df = (input_df
                .withColumn("create_date", F.current_timestamp())
                .withColumn("change_date", F.current_timestamp())
                .withColumn("src_file_process_time", F.current_timestamp())
                .withColumn("file_modification_time", F.expr("_metadata.file_modification_time"))
                .withColumn("hkey", F.lit(""))
                .withColumn("hdiff", F.lit(""))
                )
    return input_df


def rename_cols(df, table_name, col_mapping_config_table):
    config_df = spark.table(col_mapping_config_table)
    config_df = config_df.filter(config_df.table_name == table_name)
    config = config_df.collect()

    for row in config:
        old_col = row.source_col_name
        new_col = row.source_target_name
        df = df.withColumnRenamed(old_col, new_col)
    return df

def display_count(df):
    print(f"Batch count: {df.count()}")
    return df


def get_distinct_vals(df):
    return df.distinct()


def handle_deletes(df):
    deletes_df = df.filter(col("ACTION")=="DELETE")
    non_deletes_df = df.filter(col("ACTION")!="DELETE")
    # Delete sql goes here
    deletes_df.createOrReplaceTempView("DELETES")
    df.sparkSession.sql(f"delete * from target_table where id in (select id from {deletes_df})")

    return non_deletes_df

# COMMAND ----------

class Args:
    def __init__(self, config_path, data_product_name,table_name,source_data_type, source_table_type, audit_write, verbose, run_dq_rules,source_filepath,pkeys, streaming, trigger, writes, transformations):
        self.config_path = config_path
        self.data_product_name = data_product_name
        self.table_name = table_name
        self.source_data_type = source_data_type
        self.source_table_type = source_table_type
        self.audit_write = audit_write
        self.verbose = verbose
        self.run_dq_rules = run_dq_rules
        self.source_filepath = source_filepath
        self.pkeys = pkeys
        self.streaming = streaming
        self.trigger = trigger
        self.writes=writes
        self.transformations=transformations


def get_user_args():

    # dbutils.widgets.text("config_path", "", "The path to the config file")
    dbutils.widgets.text("data_product_name", "", "Data Product Name is a Campbell's specific variable for their audit system")
    dbutils.widgets.text("table_name", "", "The table name for ingestion")
    dbutils.widgets.text("config_database", "", "The table name with configurations")
    # dbutils.widgets.text("source_data_type", "", "Source data type -> spark.read.format(source_data_type)")
    # dbutils.widgets.text("source_table_type", "", "Source table type is for when using streaming to specify the cloudFiles + source_table_type")
    # dbutils.widgets.dropdown("audit_write", "False", ["True", "False"], "Whether to write to the audit tables")
    # dbutils.widgets.dropdown("run_dq_rules", "False", ["True", "False"], "Whether to run data quality rules")
    # dbutils.widgets.dropdown("verbose", "True", ["True", "False"], "Whether to write out the SQL queries")
    # dbutils.widgets.text("source_filepath","","The source path for loading data")
    # dbutils.widgets.text("pkeys","","Primary Keys of target table")
    # dbutils.widgets.dropdown("source_is_streaming","False", ["True", "False"],"Streaming boolean for whether to use spark.read or the streaming reader")
    # dbutils.widgets.text("trigger","once","treaming trigger value (currently only supports trigger once or continuous)")
    # dbutils.widgets.text("writes","","Writes would be a list of write values, this allows for multiple writes per source")
    # dbutils.widgets.text("transformations","","Writes would be a list of transformations functions")


    # Retrieve the values from the widgets
    
    # config_path = dbutils.widgets.get("config_path")
    data_product_name = dbutils.widgets.get("data_product_name")
    table_name = dbutils.widgets.get("table_name")
    table_name = dbutils.widgets.get("config_database")
    # source_data_type = dbutils.widgets.get("source_data_type")
    # source_table_type = dbutils.widgets.get("source_table_type")
    # audit_write = dbutils.widgets.get("audit_write") == "True"  # Convert to boolean
    # verbose = dbutils.widgets.get("verbose") == "True"  # Convert to boolean
    # run_dq_rules = dbutils.widgets.get("run_dq_rules") == "True"
    # source_filepath = dbutils.widgets.get("source_filepath")
    # pkeys = dbutils.widgets.get("pkeys")
    # streaming = dbutils.widgets.get("source_is_streaming").lower()
    # trigger = dbutils.widgets.get("trigger").lower()
    # writes = dbutils.widgets.get("writes")
    # transformations = dbutils.widgets.get("transformations")

    config_raw_df = spark.sql(f"""select * except(rec_created_date) from {config_database} where data_product_name = '{data_product_name}' and table_name = '{table_name}'""")
    config_raw_df_pd = config_raw_df.toPandas()
    task_list = config_raw_df_pd.to_dict(orient='records')
    task_json_list = [eval(json.dumps(d)) for d in task_list]


    return Args(config_path=config_path,
                table_name=table_name,
                data_product_name=data_product_name,
                source_data_type=source_data_type,
                source_table_type=source_table_type,
                audit_write=audit_write,
                verbose=verbose,
                run_dq_rules=run_dq_rules,
                source_filepath=source_filepath,
                pkeys=pkeys,
                streaming=streaming,
                trigger=trigger,
                writes=writes,
                transformations=transformations
                )

# COMMAND ----------

get_user_args()

# COMMAND ----------

# def main():
#     # Get User Arguments
#     args = get_user_args()

#     audit_write = args.audit_write

#     if audit_write is True:
#         audit_table_config = {}
#         data_product_name = args.data_product_name
#         target_table = config.get_target_table_name()
#         source_table = config.get_source_table_name()

#         ath = AuditTableHandler(audit_table_config)

#         ath.write_audit_dim_table(target_table, data_product_name, source_table)
#         ath.signal_pipeline_start()

#     table_name = args.table_name

#     #pkeys = ["USER_CODE", "ACTIONSET_CODE", "PARAM_CODE", "PVALUE"]
#     pkeys = args.pkeys.split(",")


#     pipeline_config = {
#         # Data Product Name is a Campbell's specific variable for their audit system
#         # for keeping track of their data subsets
#         "data_product_name": args.data_product_name,

#         # Source data type -> spark.read.format(source_data_type)
#         "source_data_type": args.source_data_type,

#         # Streaming boolean for whether to use spark.read or the streaming reader
#         "streaming" : args.streaming,

#         # Source table type is for when using streaming to specify the cloudFiles + source_table_type
#         "source_table_type" : "parquet",

#         # The file source in ADLS
#         #"source_filepath": f"abfss://landing@sacscdnausedlhdev.dfs.core.windows.net/optiva/{table_name}",
#         "source_filepath":args.source_filepath,

#         # Any custom source reading options i.e. spark.read.option(**source_reader_options)
#         "source_reader_options": args.source_reader_options
#         },
#         # Streaming trigger value (currently only supports trigger once or continuous)
#         #"trigger" : "once",
#         "trigger": args.trigger

#         # Writes would be a list of write values, this allows for multiple writes per source
#         "writes": args.write,
#         "transformations": args.transformations
#     }

#     # Process the configuration (either path or dictionary)
#     config = ConfigHandler(config_path=args.config_path, config=pipeline_config).get_config()

#     # TO DO: Add functions to fncs dinamically
#     pb = PipelineBuilder(spark, config, verbose=args.verbose, 
#                          fncs=[add_timestamp_cols, display_count, get_distinct_vals,rename_cols])
    
#     if args.run_dq_rules is True:
#         dq = DataQualityChecks()

#     # Read the data
#     df = pb.read()
    
#     # Perform transformations
#     df, _ = pb.run(df)

#     # Run Data Quality Checks
#     if args.run_dq_rules is True:
#         dq_config = get_dq_config(table="dq_database_eval_quality")
#         active_valid_df, active_invalid_df, passive_valid_df, passive_invalid_df = dq.run(df, dq_config)

#     # Write output
#     pb.write(df)

#     if audit_write:
#         # Write the type 1 updates and the target table count to the audit table
#         ath.write_type1_updates(df)
#         ath.write_target_table_count(target_table)
#         ath.signal_pipeline_end()


# if __name__ == "__main__":
#     main()