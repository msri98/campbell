# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType



def read_parquet(spark, path, reader_config=None):
  
    if reader_config is not None:
        return (
            (spark.read
            .options(**reader_config)
            .parquet(path)
            .withColumn("file_modification_time", F.col("_metadata.file_modification_time"))
            .withColumn("file_path", F.col("_metadata.file_path"))
            .withColumn("is_current",F.lit(1))
            .withColumn("is_deleted",F.lit(0))
            .withColumn("start_time",F.current_timestamp())
            .withColumn("end_time",F.lit(None).cast(TimestampType()))
            .withColumn("row_creation_time",F.current_timestamp())
            ).orderBy(['file_modification_time'], ascending = [True])
        )
    else:
        return (
            (spark.read
            .parquet(path)
            .withColumn("file_modification_time", F.col("_metadata.file_modification_time"))
            .withColumn("file_path", F.col("_metadata.file_path"))
            .withColumn("read_creation_time",F.current_timestamp())
            .withColumn("is_deleted",F.lit(0))
            ).orderBy(['file_modification_time'], ascending = [True])
        )

def read_csv(spark, path, reader_config=None):
    if reader_config is not None:
        return spark.read.options(**reader_config).csv(path)
    else:
        return spark.read.csv(path)


def read_json(spark, path, reader_config=None):
    if reader_config is not None:
        return spark.read.options(**reader_config).json(path)
    else:
        return spark.read.json(path)


def read_orc(spark, path, reader_config=None):
    if reader_config is not None:
        return spark.read.options(**reader_config).orc(path)
    else:
        return spark.read.orc(path)


def read_hms_table(spark, table_name, read_config=None):
    if read_config is not None:
        raise NotImplementedError("Read config is not supported for HMS tables")
    if not isinstance(table_name, str):
        raise ValueError("table_name must be a string")
    if '.' not in table_name:
        table_name = f'default.{table_name}'
    return spark.read.table(table_name)


def read_uc_table(spark, table_name, read_config=None):
    if read_config is not None:
        raise NotImplementedError("Read config is not supported for UC tables")
    if not isinstance(table_name, str):
        raise ValueError("table_name must be a string")
    if not '.' in table_name:
        raise ValueError("table_name must be in the format 'catalog.schema.table'")
    if not table_name.count('.') == 2:
        raise ValueError("table_name must be in the format 'catalog.schema.table'")
    return spark.read.table(table_name)


def read_delta_path(spark, path, reader_config=None):
    if reader_config is not None:
        return spark.read.options(**reader_config).format("delta").load(path)
    else:
        return spark.read.format("delta").load(path)
