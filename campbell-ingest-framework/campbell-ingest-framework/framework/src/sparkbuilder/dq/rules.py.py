# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class DataQualityChecks(object):

    def __init__(self):
        self.check_mapping = {
            "null_check": self.check_nulls,
            "range_check": self.check_range,
            "orphan_check": self.check_orphan,
            "unique_check": self.check_unique,
            "datatype_check": self.check_datatype,
            "special_character_check": self.check_special_character,
            "date_format_check": self.check_date_format,
            "fixed_length_check": self.check_fixed_length,
            "valid_values_check": self.check_valid_values
        }

    def check_nulls(self, df, check_config):
        cols = check_config["columns"]
        for col in cols:
            df = df.withColumn(col + "_null_check", F.when(F.col(col).isNull(), 1).otherwise(0))
        valid_df = df.where(F.col(col + "_null_check") == 0)
        invalid_df = df.where(F.col(col + "_null_check") == 1)
        invalid_df = invalid_df.withColumn("invalid_reason", F.lit(f"Null values in columns {cols}"))
        return valid_df, invalid_df

    def check_range(self, df, check_config):
        cols = check_config["columns"]
        rules = check_config["rules"]
        for col in cols:
            for rule in rules:
                if rule["type"] == "lower_than":
                    df = df.withColumn(col + "_range_check", F.when(F.col(col) < rule["threshold"], 1).otherwise(0))
                elif rule["type"] == "higher_than":
                    df = df.withColumn(col + "_range_check", F.when(F.col(col) > rule["threshold"], 1).otherwise(0))
                elif rule["type"] == "invalid_range":
                    df = df.withColumn(col + "_range_check",
                                       F.when((F.col(col) < rule["range"][0]) | (F.col(col) > rule["range"][1]),
                                              1).otherwise(0))
                elif rule["type"] == "valid_range":
                    df = df.withColumn(col + "_range_check",
                                       F.when((F.col(col) >= rule["range"][0]) & (F.col(col) <= rule["range"][1]),
                                              1).otherwise(0))
                elif rule["type"] == "mandatory":
                    df = df.withColumn(col + "_range_check", F.when(F.col(col).isNull(), 1).otherwise(0))
        valid_df = df.where(F.col(col + "_range_check") == 0)
        invalid_df = df.where(F.col(col + "_range_check") == 1)
        valid_df = valid_df.drop(*[col + "_range_check" for col in cols])
        invalid_df = invalid_df.drop(*[col + "_fixed_length_check" for col in cols])
        invalid_df = invalid_df.withColumn("invalid_reason", F.lit(f"Invalid range in columns {cols}"))
        return valid_df, invalid_df

    def check_orphan(self, df, check_config):
        cols = check_config["columns"]
        df = df.withColumn("orphan_check", F.when(F.col(cols[0]).isNull(), 1).otherwise(0))
        valid_df = df.where(F.col("orphan_check") == 0)
        invalid_df = df.where(F.col("orphan_check") == 1)
        valid_df = valid_df.drop(*[col + "orphan_check" for col in cols])
        invalid_df = invalid_df.drop(*[col + "orphan_check" for col in cols])
        invalid_df = invalid_df.withColumn("invalid_reason", F.lit(f"Orphan values in columns {cols}"))
        return valid_df, invalid_df

    def check_unique(self, df, check_config):
        cols = check_config["columns"]
        df = df.withColumn("unique_check", F.row_number().over(Window.partitionBy(cols).orderBy(F.lit(1))))
        valid_df = df.where(F.col("unique_check") == 1)
        invalid_df = df.where(F.col("unique_check") != 1)
        valid_df = valid_df.drop(*[col + "unique_check" for col in cols])
        invalid_df = invalid_df.drop(*[col + "unique_check" for col in cols])
        invalid_df = invalid_df.withColumn("invalid_reason", F.lit(f"Duplicate values in columns {cols}"))
        return valid_df, invalid_df

    def check_datatype(self, df, check_config):
        cols = check_config["columns"]
        datatype = check_config["datatype"]
        for col in cols:
            df = df.withColumn(col + "_datatype_check", F.when(F.col(col).cast(datatype).isNull(), 1).otherwise(0))
        valid_df = df.where(F.col(col + "_datatype_check") == 0)
        invalid_df = df.where(F.col(col + "_datatype_check") == 1)
        valid_df = valid_df.drop(*[col + "_datatype_check" for col in cols])
        invalid_df = invalid_df.drop(*[col + "_datatype_check" for col in cols])
        invalid_df = invalid_df.withColumn("invalid_reason", F.lit(f"Invalid datatype in columns {cols}"))
        return valid_df, invalid_df

    def check_special_character(self, df, check_config):
        cols = check_config["columns"]
        for col in cols:
            df = df.withColumn(col + "_special_character_check",
                               F.when(F.col(col).rlike("[^a-zA-Z0-9]"), 1).otherwise(0))
        valid_df = df.where(F.col(col + "_special_character_check") == 0)
        invalid_df = df.where(F.col(col + "_special_character_check") == 1)
        valid_df = valid_df.drop(*[col + "_special_character_check" for col in cols])
        invalid_df = invalid_df.drop(*[col + "_special_character_check" for col in cols])
        invalid_df = invalid_df.withColumn("invalid_reason", F.lit(f"Special characters found in columns {cols}"))
        return valid_df, invalid_df

    def check_date_format(self, df, check_config):
        cols = check_config["columns"]
        date_format = check_config["format"]
        for col in cols:
            df = df.withColumn(col + "_date_format_check",
                               F.when(F.to_date(F.col(col), date_format).isNull(), 1).otherwise(0))

        valid_df = df.where(F.col(col + "_date_format_check") == 0)
        invalid_df = df.where(F.col(col + "_date_format_check") == 1)
        valid_df = valid_df.drop(*[col + "_date_format_check" for col in cols])
        invalid_df = invalid_df.drop(*[col + "_date_format_check" for col in cols])
        invalid_df = invalid_df.withColumn("invalid_reason", F.lit(f"Invalid date format in columns {cols}"))
        return valid_df, invalid_df

    def check_fixed_length(self, df, check_config):
        cols = check_config["columns"]
        length = check_config["length"]
        for col in cols:
            df = df.withColumn(col + "_fixed_length_check", F.when(F.length(F.col(col)) != length, 1).otherwise(0))
        valid_df = df.where(F.col(col + "_fixed_length_check") == 0)
        invalid_df = df.where(F.col(col + "_fixed_length_check") == 1)
        valid_df = valid_df.drop(*[col + "_fixed_length_check" for col in cols])
        invalid_df = invalid_df.drop(*[col + "_fixed_length_check" for col in cols])
        invalid_df = invalid_df.withColumn("invalid_reason", F.lit(f"Invalid length in columns {cols}"))
        return valid_df, invalid_df

    def check_valid_values(self, df, check_config):
        cols = check_config["columns"]
        valid_values = check_config["valid_values"]
        for col in cols:
            df = df.withColumn(col + "_valid_values_check", F.when(~F.col(col).isin(valid_values), 1).otherwise(0))
        valid_df = df.where(F.col(col + "_valid_values_check") == 0)
        invalid_df = df.where(F.col(col + "_valid_values_check") == 1)
        valid_df = valid_df.drop(*[col + "_valid_values_check" for col in cols])
        invalid_df = invalid_df.drop(*[col + "_valid_values_check" for col in cols])
        invalid_df = invalid_df.withColumn("invalid_reason", F.lit(f"Invalid values in columns {cols}"))
        return valid_df, invalid_df

    def _run_checks(self, df, config):
        """
        Run passive checks on the dataframe
        :param df: 
        :param config: 
        :return: 
        """
        invalid_dfs = None
        for check_config in config:
            check_type = check_config["type"]
            if check_type in self.check_mapping:
                valid_df, invalid_df = self.check_mapping[check_type](df, check_config)
                if invalid_dfs is None:
                    invalid_dfs = invalid_df
                else:
                    invalid_dfs = invalid_dfs.union(invalid_df)
                df = valid_df
        return df, invalid_dfs

    def run(self, df, config):
        active_df = None
        active_invalid_df = None
        passive_df = None
        passive_invalid_df = None
        if "active_checks" in config:
            active_df, active_invalid_df = self._run_checks(df, config["active_checks"])
        if "passive_checks" in config:
            passive_df, passive_invalid_df = self._run_checks(df, config["passive_checks"])
        if not "active_checks" in config and not "passive_checks" in config:
            return df, None, df, None
        elif not "active_checks" in config:
            return df, None, passive_df, passive_invalid_df
        elif not "passive_checks" in config:
            return active_df, active_invalid_df, None, None
        else:
            return active_df, active_invalid_df, passive_df, passive_invalid_df
