# Databricks notebook source
#from sparkbuilder.writers.batch_writers import append_write

def streaming_write_table_append(spark,df,write_config):
    
    if  not ("catalog" in write_config[0] and "schema" in write_config[0] and "table" in write_config[0]) :
       
        raise Exception("Exception in the function sparkbuilder.writers.streaming_writers.streaming_write_table_append: catalog/schema/table is not defined in the writes field in the configuration table.")
    
    
    if "mergeSchema" in write_config[0]:
        merge_schema=write_config[0]["mergeSchema"].lower() 
    else:
        #default value
        merge_schema='false'
    
    table_name=f"""{write_config[0]["catalog"]}.{write_config[0]["schema"]}.{write_config[0]["table"]}""".lower()
    target_table_type=write_config[0]["target_table_type"].lower()
    checkpoint_filepath=write_config[0]["checkpoint_filepath"]
    output_mode=write_config[0]["mode"].lower()
    trigger=write_config[0]["trigger"]
   

    try:
        sap_streaming_query = (df
                                .writeStream
                                .format(target_table_type)
                                .option("checkpointLocation",checkpoint_filepath)
                                .outputMode(output_mode)
                                .option("mergeSchema",merge_schema)
                                .trigger(availableNow=True)
                                .toTable(table_name)
                            )
        
        for query in spark.streams.active:
            print(f"Waiting for query {query.id} to complete ...")
            query.awaitTermination()
        
    except Exception as e:
        print(f"Exception in sparkbuilder.writers.streaming_writers.streaming_write_table_append srini => {str(e)}") 

# def merge_write(spark, df, config, table_type, verbose=False, target_table=None):
#     print(f"Merge write config: {config}")
#     if not isinstance(df, DataFrame):
#         raise ValueError("df must be a DataFrame")
#     if not isinstance(config, dict):
#         raise ValueError("config must be a dictionary")
#     if not isinstance(table_type, str):
#         raise ValueError("table_type must be a string")
#     if not "keys" in config:
#         raise ValueError("keys must be specified in the config for merge")

#     # spark = SparkSession.builder.getOrCreate()
#     # spark = df.sparkSession

#     if "timestamp_col" in config:
#         df = df.withColumn(config["timestamp_col"], F.current_timestamp())

#     # TODO: Check for keys
#     keys = config["keys"]

#     if not isinstance(keys, list):
#         raise ValueError("Primary Keys must be a list")
#     if len(keys) == 0:
#         raise ValueError("Primary Keys must be specified")

#     for k in keys:
#         if not isinstance(k, str):
#             raise ValueError("Primary Keys must be strings")
#         # if not k.lower() in [col.lower() for col in df.columns]:
#         #     raise ValueError(f"Key {k} not found in DataFrame")

#     join_statement = " AND ".join([f"target.{k} = source.{k}" for k in keys])

#     if 'except_column_list' in config:
#         except_column_list = config["except_column_list"]
#         if not isinstance(except_column_list, list):
#             raise ValueError("Except Column List must be a list")
#         if len(except_column_list) != 0:
#             df = df.drop(*except_column_list)

#     if target_table is None:
#         if 'catalog' in config:
#             target_table = f"{config['catalog']}.{config['schema']}.{config['table']}"
#         elif 'target_database' in config:
#             target_table = f"{config['target_database']}.{config['table']}"
#         else:
#             raise ValueError("table must be specified in the config")

#     if "scd_type" in config:
#         scd_type = int(config["scd_type"])
#     else:
#         scd_type = 1

#     if not (scd_type in [1, 2]):
#         raise ValueError("SCD Type must be 1 or 2")

#     if not _table_exists(df.sparkSession, target_table):
#         #overwrite_write(df, config, table_type)
#         df.write.format('delta').saveAsTable(target_table)
#         return None
#     else:
#         if scd_type == 2:
#             df = df.withColumn("is_current", F.lit(1))
#             df = df.withColumn("start_ts", F.current_timestamp())
#             df = df.withColumn("end_ts", F.lit(None).cast("timestamp"))
#         df.createOrReplaceTempView("updates")

#     queries = []
#     if scd_type == 1:
#         merge_query = f"""
#                     MERGE INTO {target_table} AS target
#                     USING updates AS source
#                     ON {join_statement}
#                     WHEN MATCHED THEN
#                         UPDATE SET *
#                     WHEN NOT MATCHED THEN
#                         INSERT *
#                     """
#         spark.sql(merge_query)
#     elif scd_type == 2:
#         update_query = f"""
#         MERGE INTO {target_table} AS target
#                     USING updates AS source
#                     ON {join_statement}
#                     WHEN MATCHED THEN
#                         UPDATE SET is_current = 0, end_ts = current_timestamp()
#         """

#         queries.append(update_query)
#         df.sparkSession.sql(update_query)
#         # TODO: Handle more than just tables
#         df.write.format("delta").mode("append").saveAsTable(target_table)

# def streaming_merge(spark, df, config, table_type):
#     merge_udf = lambda batch_df, batch_id: merge_write(spark, batch_df, config, table_type)
#     (df.writeStream
#      .format("delta")
#      .foreachBatch(merge_udf)
#      .outputMode("update")
#      .start()
#      )


# def _streaming_write_table(df, config, table_type, mode):
#     if not table_type in ["delta", "parquet", "csv"]:
#         raise ValueError("table_type must be one of 'delta', 'parquet', or 'csv")
#     if not mode in ["append", "overwrite"]:
#         raise ValueError("mode must be one of 'append' or 'overwrite")
#     if "trigger" in config:
#         trigger = config["trigger"]
#         if trigger == "once":
#             trigger_once=True
#         else:
#             trigger_once=False
#     if "processingTimestr" in config:
#         processingTimestr = config['processingTimestr']
#     else:
#         processingTimestr = None
#     if trigger_once is True:
#         (df.writeStream
#         .format(table_type)
#         .outputMode(mode)
#         .trigger(once=trigger_once)
#         )
#     elif isinstance(processingTimestr, str):
#         (df.writeStream
#         .format(table_type)
#         .outputMode(mode)
#         .trigger(processingTimestr=processingTimestr)
#         )
#     else:
#         (df.writeStream
#         .format(table_type)
#         .outputMode(mode)
#         )


# def _streaming_write_file(df, config, table_type, mode):
#     if not table_type in ["delta", "parquet", "csv"]:
#         raise ValueError("table_type must be one of 'delta', 'parquet', or 'csv")
#     if not mode in ["append", "overwrite"]:
#         raise ValueError("mode must be one of 'append' or 'overwrite")
#     (df.writeStream
#      .format(table_type)
#      .outputMode(mode)
#      .option("checkpointLocation", "/tmp/delta/_checkpoints/")
#      .start("/delta/events")
#      )

# def streaming_write_file_append(df, config, table_type):
#     (df.writeStream
#      .format("delta")
#      .foreachBatch(merge_udf)
#      .outputMode("update")
#      .start()
#      )
#     _streaming_write_file(df, config, table_type, "append")

# def streaming_write_file_overwrite(df, config, table_type):
#     overwrite_udf = lambda batch_df, batch_id: _streaming_write_file(df, config, table_type, "overwrite")
#     (df.writeStream
#      .format("delta")
#      .foreachBatch(overwrite_udf)
#      .outputMode("update")
#      .start()
#      )

