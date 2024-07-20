# Databricks notebook source
from pyspark.sql import functions as F
from sparkbuilder.writers.batch_writers import merge_write #, overwrite_write, append_write 
from sparkbuilder.writers.streaming_writers import streaming_write_table_append #,streaming_write_file_overwrite, streaming_write_file_append, 

class Writer(object):

    def __init__(self, spark, config):
        """
        Writer class for writing dataframes to a target location
        :param config:
        """
        if not isinstance(config, dict):
            raise ValueError("config must be a dictionary")
        self.config = config
        self.spark = spark
        self.control_columns_list = ["row_creation_time", "file_modification_time", "file_path", "start_time", "end_time", "is_current","is_deleted"]
    
    def brute_force_merge_writer(self,df_source,writer_config,scd_type,table_type):

        if scd_type != 2:
            raise Exception("Exception: for Optiva SCD Type 1 Batch Mode is not Implemented yet.")
        
        catalog =  writer_config["catalog"]
        schema =  writer_config["schema"]
        table =  writer_config["table"]

        source_table_name = self.config["table_name"]
        target_table = f"{catalog}.{schema}.{table}"
        pkeys = self.config["keys"]
                    
        if not self._table_exists(target_table):
            self._create_table_by_scd_type(df_source, target_table, scd_type, writer_config)
      
           
        df_target=self.spark.sql(f"select * from {target_table} where is_deleted=0 and is_current=1")
            

        control_cols=["start_time","end_time","is_current","is_deleted","row_creation_time","file_modification_time","file_path"]

        scd_columns = list(set(df_source.columns) - set(pkeys) - set(control_cols))

        df_joined=df_source.alias("source").join(df_target.alias("target"),on=pkeys, how='fullouter')

        #conditions=[F.col(c) != F.col(c + "_target") for c in scd_columns]
        conditions=[F.col("source."+c) != F.col("target."+c) for c in scd_columns]

        filter_condition=conditions[0]
        for condition in conditions[1:]:
            filter_condition=filter_condition | condition

        source_cols=[F.col("source."+c) for c in df_source.columns] + ["row_flag"]

        # # Updated rows
        df_changed=df_joined.filter(filter_condition).withColumn("row_flag",F.lit("UPDATED"))
        df_changed=df_changed.select(*source_cols)

        upd_temporary_table=f"{table}_temp_vw"
        df_changed.createOrReplaceTempView(f"{upd_temporary_table}")
      
        where_upd_clause = " AND ".join([ c for c in pkeys]) 
        upd_select_clause=" , ".join([ 'target.'+ c for c in pkeys])
        join_clause = " AND ".join(['target.' + c + '=source.'+c for c in pkeys])

        ## for incoming updated rows, expire the counter part row in the target table
        update_upd_sql = f"""UPDATE {target_table}  SET {target_table}.is_current=0, {target_table}.end_time=current_timestamp() WHERE {where_upd_clause}  IN (SELECT  { upd_select_clause}  FROM {target_table} AS target JOIN {upd_temporary_table}  as SOURCE ON {join_clause}  where target.end_time is null and target.is_current=1) and {target_table}.is_current=1 and {target_table}.end_time is null"""
      
        self.spark.sql(f"""{update_upd_sql}""")


        # inserting incoming updated rows into target
        update_ins_sql = f"""INSERT INTO {target_table} select {",".join([col for col in df_source.columns])} from {upd_temporary_table}""".replace("start_time","greatest(CREATION_DATE,MODIFY_DATE) as start_time")

        self.spark.sql(f"""{update_ins_sql}""")

        
        #Inserting new records

        df_new=df_joined.where(F.col("target.file_modification_time").isNull()).withColumn("row_flag",F.lit("NEW"))
        df_new=df_new.select(*source_cols)

        temp_inserts_vw=f"{table}_ins_temp"
        df_new.createOrReplaceTempView(f"{temp_inserts_vw}")

        inserts_sql=f"""INSERT INTO {target_table} SELECT {",".join([col for col in df_source.columns])} FROM  {temp_inserts_vw}"""
        inserts_sql=inserts_sql.replace("start_time","greatest(CREATION_DATE,modify_date) as start_time")
        
        self.spark.sql(f"""{inserts_sql}""")


        # Mark the deleted rows in the target table
        deletes_temp_vw=f"{table}_deletes_vw"
        df_joined.filter(F.col("source.file_modification_time").isNull() & F.col("target.file_modification_time").isNotNull()).createOrReplaceTempView(f"{deletes_temp_vw}")


        #delete_conditions="&".join([F.col("source."+c).isNull() for c in pkeys])

        delete_update_sql = f"""UPDATE {target_table}  SET {target_table}.is_current=False, {target_table}.is_deleted=1,end_time=current_timestamp() WHERE {where_upd_clause}  IN (SELECT  { upd_select_clause}  FROM {target_table} AS target JOIN {deletes_temp_vw}  as SOURCE ON {join_clause}  where target.end_time is null and target.is_current=1 and target.is_deleted=0) and {target_table}.is_current=1 and {target_table}.end_time is null"""
        
        #Execute the Query
        self.spark.sql(f"""{delete_update_sql}""")

    def _overwrite_delta(self, batch_df, batch_id, keys, source_orderBy_column, target_table, scd_type):
        batch_df.createOrReplaceTempView("updates")
        over_partition_columns = ", ".join([ k for k in keys])
        control_columns = ", ".join([ k for k in self.control_columns_list])

        base_df = batch_df.sparkSession.sql(f"""
            SELECT *, 
                ROW_NUMBER() OVER(PARTITION BY {over_partition_columns} ORDER BY {source_orderBy_column} DESC) as rn
            FROM updates
        """)

        preprocessed_df = base_df.where(f"rn = 1").withColumn(
            "end_time", F.when(F.col("rn") != 1, F.current_timestamp()).otherwise(None)
        ).withColumn(
            "is_current", F.when(F.col("rn") != 1, 0).otherwise(1)
        )

        if scd_type == 1:
            preprocessed_df.drop(*self.control_columns_list).drop('rn').mode("overwrite").save(target_table)

        elif scd_type == 2:
            preprocessed_df.drop('rn').mode("overwrite").save(target_table)

        else:
            raise Exception("Missing scd_type in function _first_insert_to_delta")
        
        batch_df.sparkSession.sql(update_query)

    def _first_insert_to_delta(self, batch_df, batch_id, keys, source_orderBy_column, target_table, scd_type):
        batch_df.createOrReplaceTempView("updates")
        over_partition_columns = ", ".join([ k for k in keys])
        control_columns = ", ".join([ k for k in self.control_columns_list])

        if scd_type == 1:
            update_query = f"""
                WITH BasePreprocessedSource AS (
                SELECT * except(is_current,end_time),
                    ROW_NUMBER() OVER(PARTITION BY {over_partition_columns} ORDER BY {source_orderBy_column} DESC) as rn
                FROM updates
                ),
                PreprocessedSource AS (
                    SELECT *,
                        CASE
                            WHEN rn != 1 THEN current_timestamp() 
                            ELSE null
                        END AS end_time,
                        CASE 
                            WHEN rn != 1 THEN 0 
                            ELSE 1 
                        END AS is_current
                    FROM BasePreprocessedSource
                )
                INSERT INTO {target_table}
                    SELECT * EXCEPT({control_columns}, rn) from PreprocessedSource where is_current = 1
            """

        elif scd_type == 2:
            update_query = f"""
                WITH BasePreprocessedSource AS (
                SELECT * except(is_current,end_time),
                    ROW_NUMBER() OVER(PARTITION BY {over_partition_columns} ORDER BY {source_orderBy_column} DESC) as rn
                FROM updates
                ),
                PreprocessedSource AS (
                    SELECT *,
                        CASE
                            WHEN rn != 1 THEN current_timestamp() 
                            ELSE null
                        END AS end_time,
                        CASE 
                            WHEN rn != 1 THEN 0 
                            ELSE 1 
                        END AS is_current
                    FROM BasePreprocessedSource
                )
                INSERT INTO {target_table}
                    SELECT * EXCEPT(rn) from PreprocessedSource
            """
        else:
            raise Exception("Missing scd_type in function _first_insert_to_delta")
        
        batch_df.sparkSession.sql(update_query)

        if batch_id % 101 == 0:
            # this is called inline optimization and it happens here because the streaming is continuous
            # whitout row-level concurrency enabled, merge and optmize could conflict
            # https://docs.databricks.com/en/optimizations/isolation-level.html#enable-row-level-concurrency
            self.spark.sql(f"optimize {target_table}")
            print(f"Optimizing {target_table} based on batch_id")


    def _table_exists(self, table_name):
        return self.spark.catalog.tableExists(table_name)

    def _create_external_table_from_df(self, df, table_name, location):
        schema = df.schema
        columns = []
        
        for field in schema.fields:
            columns.append(f"`{field.name}` {field.dataType.simpleString()}")
        
        columns_str = ",\n".join(columns)
        
        create_table_sql = f"""
        CREATE TABLE {table_name} (
        {columns_str}
        )
        USING delta
        LOCATION '{location}'
        """
      
        self.spark.sql(create_table_sql) 
        

    def _create_table_by_scd_type(self, df, target_table, scd_type, writer_config):
        if scd_type == 1:
            new_table_df = self.spark.createDataFrame([], df.schema).drop(*self.control_columns_list)
        elif scd_type == 2:
            new_table_df = self.spark.createDataFrame([], df.schema)
        else:
            raise Exception("Missing scd_type in function _create_table_by_scd_type")
        
        # Handle Managed and External tables
        if "external_location" in writer_config and writer_config["external_location"] != None and writer_config["external_location"] != "":
            self._create_external_table_from_df(new_table_df, target_table, writer_config["external_location"])
        else:
            #managed table
            new_table_df.writeTo(target_table).create()

    
    def _upsert_to_delta(self, batch_df, batch_id, keys, source_orderBy_column, target_table, scd_type):
        batch_df.createOrReplaceTempView("updates")
        over_partition_columns = ", ".join([ k for k in keys])
        control_columns = ", ".join([ k for k in self.control_columns_list])
        join_statement = " AND ".join([f"target.{k} = source.{k}" for k in keys])

        if scd_type == 1:
            update_query = f"""
                WITH BasePreprocessedSource AS (
                SELECT *,
                    ROW_NUMBER() OVER(PARTITION BY {over_partition_columns} ORDER BY {source_orderBy_column} DESC) as rn
                FROM updates
                )
                MERGE INTO {target_table} AS target
                USING (SELECT * EXCEPT({control_columns}, rn) FROM BasePreprocessedSource WHERE rn = 1) AS source
                ON {join_statement}
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
            """
            batch_df.sparkSession.sql(update_query)

        elif scd_type == 2:
            base_df = batch_df.sparkSession.sql(f"""
                SELECT *, 
                    ROW_NUMBER() OVER(PARTITION BY {over_partition_columns} ORDER BY {source_orderBy_column} DESC) as rn
                FROM updates
            """)

            max_rn = base_df.agg({"rn": "max"}).collect()[0][0]

            for current_rn in range(max_rn, 0, -1):
                preprocessed_df = base_df.where(f"rn = {current_rn}").withColumn(
                    "end_time", F.when(F.col("rn") != 1, F.current_timestamp()).otherwise(None)
                ).withColumn(
                    "is_current", F.when(F.col("rn") != 1, 0).otherwise(1)
                )

                preprocessed_df.createOrReplaceTempView("PreprocessedSource")

                # Update existing records
                update_query = f"""
                    MERGE INTO {target_table} AS target
                    USING (SELECT * EXCEPT(rn) FROM PreprocessedSource) AS source
                    ON {join_statement}
                    WHEN MATCHED THEN
                        UPDATE SET
                            target.end_time = current_timestamp(),
                            target.is_current = 0
                """
                preprocessed_df.sparkSession.sql(update_query)
                
                # Insert all records
                update_query = f"""
                    INSERT INTO {target_table}
                        SELECT * EXCEPT(rn) from PreprocessedSource
                """
                preprocessed_df.sparkSession.sql(update_query)

        else:
            raise Exception("Missing scd_type in function _upsert_to_delta")

        if batch_id % 101 == 0:
            # this is called inline optimization and it happens here because the streaming is continuous
            # whitout row-level concurrency enabled, merge and optmize could conflict
            # https://docs.databricks.com/en/optimizations/isolation-level.html#enable-row-level-concurrency
            self.spark.sql(f"optimize {target_table}")
            print(f"Optimizing {target_table} based on batch_id")


    def streaming_merge(self, df, writer_config, table_type):
        catalog =  writer_config["catalog"]
        schema =  writer_config["schema"]
        table =  writer_config["table"]
        source_table_name = self.config["table_name"]
        target_table = f"{catalog}.{schema}.{table}"
        keys = self.config["keys"]
        source_orderBy_column = self.config["source_orderBy_column"]
        
        if "scd_type" in writer_config:
            scd_type = int(writer_config["scd_type"])
        else:
            scd_type = 1
        
        upsert_data_udf = None
        
        if not self._table_exists(target_table):
            self._create_table_by_scd_type(df, target_table, scd_type, writer_config)
    
            upsert_data_udf = lambda batch_df, batch_id: self._first_insert_to_delta(batch_df=batch_df, batch_id=batch_id, keys=keys, source_orderBy_column=source_orderBy_column, target_table=target_table, scd_type=scd_type)

            print(f"""Streaming writer for source table {source_table_name}: Creating table {target_table}""")    
            # TODO: Enable withWatermark later
            #input_stream_with_watermark = df.withWatermark(source_orderBy_column, "10 minutes")
        
        else:
            print(f"""Streaming writer for source table {source_table_name}: Merge data to table {target_table}""")    
            upsert_data_udf = lambda batch_df, batch_id: self._upsert_to_delta(batch_df=batch_df, batch_id=batch_id, keys=keys, source_orderBy_column=source_orderBy_column, target_table=target_table, scd_type=scd_type)

        query = (
            df.writeStream
            .format(table_type)
            .trigger(availableNow=True)
            .foreachBatch(upsert_data_udf)
            .option("checkpointLocation", writer_config["checkpointLocation"])
            .start()
        )
        query.awaitTermination()
    
    # TODO: Review
    def streaming_write_file_overwrite(self, df, writer_config, table_type):
        catalog =  writer_config["catalog"]
        schema =  writer_config["schema"]
        table =  writer_config["table"]
        source_table_name = self.config["table_name"]
        target_table = f"{catalog}.{schema}.{table}"
        
        if "scd_type" in writer_config:
            scd_type = int(writer_config["scd_type"])
        else:
            scd_type = 1
        
        if scd_type == 1:
            df = df.drop(*self.control_columns_list)
        
        #upsert_data_udf = lambda batch_df, batch_id: self._overwrite_delta(batch_df=batch_df, batch_id=batch_id, keys=keys, source_orderBy_column=source_orderBy_column, target_table=target_table, scd_type=scd_type)
        
        if not self._table_exists(target_table):
            self._create_table_by_scd_type(df, target_table, scd_type, writer_config)
    
            print(f"""Streaming writer for source table {source_table_name}: Creating table {target_table}""")    
        
        else:
            print(f"""Streaming writer for source table {source_table_name}: Overwriting table {target_table}""")    

        query = (
            df.write
            .format(table_type)
            #.trigger(availableNow=True)
            #.outputMode("overwrite")
            #.foreachBatch(upsert_data_udf)
            .option("checkpointLocation", writer_config["checkpointLocation"])
            #.start()
            .writeTo(target_table)
            .createOrReplace()
        )
        query.awaitTermination()


    def write(self, df):
        """
        Write a dataframe to a target location
        :param df:
        :return:
        """
        c = self.config.copy()

        print(c)
        streaming = False
        if "streaming" in c:
            if c["streaming"].lower() == "true":
                streaming = True
                print("Creating Streaming writer...")

        if not ("writes" in c):
            raise ValueError("writes must be specified in the config")
        if not isinstance(c["writes"], list):
            raise ValueError("writes must be a list")

        for wc in c["writes"]:
            if "target_table_type" not in wc:
                table_type = "delta"
            else:
                table_type = wc["target_table_type"]
            print(f"- Writing to table type: {table_type}")

            if not 'mode' in wc:
                write_mode = "append"
            else:
                write_mode = wc["mode"]

            if not write_mode in ["overwrite", "append", "merge","brute_force_merge"]:
                raise ValueError("mode must be one of 'overwrite', 'append', or 'merge' or 'brute_force_merge'")

            if (streaming and write_mode=="append" and 
                table_type=="delta" and 
                self.config["data_product_name"].lower()=="sap_cdc"):

                streaming_write_table_append(self.spark,df,c["writes"])
         

            if streaming and self.config["data_product_name"].lower()=="optiva":
                if write_mode == "merge":
                    self.streaming_merge(df, wc, table_type)
                # elif write_mode == "overwrite":
                #     self.streaming_write_file_overwrite(df, wc, table_type)
                # elif write_mode == "append":
                #     streaming_write_file_append(df, wc, table_type)
        
            elif not streaming and self.config["data_product_name"].lower()=="optiva":
                if write_mode == "merge":
                    merge_write(self.spark, df, wc, table_type)
                elif write_mode== 'brute_force_merge':
                    self.brute_force_merge_writer(df,wc,wc["scd_type"],table_type)   
                      
                #elif write_mode == "overwrite":
                #    overwrite_write(df, wc, table_type)
                #elif write_mode == "append":
                #    append_write(df, wc, table_type)
