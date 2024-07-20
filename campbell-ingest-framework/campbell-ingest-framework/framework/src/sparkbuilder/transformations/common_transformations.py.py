# Databricks notebook source
import pyspark.sql.functions as F
import re

def run_where_clause(df, transform_config):
    if not isinstance(transform_config, dict):
        raise ValueError("transform_config must be a dictionary")
    where_exp = transform_config["where"]
    return df.where(*where_exp)


def run_select_clause(df, transform_config):
    if not isinstance(transform_config, dict):
        raise ValueError("transform_config must be a dictionary")
    select_cols = transform_config["select"]
    if not isinstance(select_cols, list):
        raise ValueError("select cols value must be a list")
    for c in select_cols:
        if not isinstance(c, str):
            raise ValueError("select cols must be a list of strings")
    return df.select(*select_cols)


def run_drop_columns(df, transform_config):
    columns = transform_config["drop"]
    if not isinstance(columns, list):
        raise ValueError("columns must be a list")
    return df.drop(*columns)


def run_rename_columns(df, transform_config):
    column_map = transform_config["rename"]
    if not isinstance(column_map, dict):
        raise ValueError("column_map must be a dictionary")
    for k, v in column_map.items():
        if not isinstance(k, str):
            raise ValueError("column_map keys must be strings")
        if not isinstance(v, str):
            raise ValueError("column_map values must be strings")
    for k, v in column_map.items():
        df = df.withColumnRenamed(k, v)
    return df


def run_normalize_cols(input_df):
    for each in input_df.schema.names:
        input_df = input_df.withColumnRenamed(
            each, re.sub(r"\s+([a-zA-Z_][a-zA-Z_0-9]*)\s*", "", each.replace(" ", "_"))
        )
    return input_df


