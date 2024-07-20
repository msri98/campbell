# Databricks notebook source

def get_dq_config(table="dq_database_eval_quality"):
    dq_rules = spark.table(table).collect()
    configs = {"active": [], "passive": []}
    for row in dq_rules:
        if row.implement_type == 'Post_Update':
            if row.rule_type_desc == 'Null Check':
                configs[row.type].append(
                        {
                            "type": "null_check",
                            "columns": [row.column_name]
                        }
                    )

            elif row.rule_type_desc == 'Range Check':
                configs[row.type].append(
                        {
                            "type": "range_check",
                            "columns": [row.column_name],
                            "rules": [
                                {
                                    "type": "lower_than",
                                    "threshold": row.threshold_low
                                },
                                {
                                    "type": "higher_than",
                                    "threshold": row.threshold_high
                                }
                            ]
                        }
                    )
            elif row.rule_type_desc == 'Unique Check':
                configs[row.type].append(
                        {
                            "type": "unique_check",
                            "columns": [row.column_name]
                        }
                    )
            elif row.rule_type_desc == 'Data Type Check':
                configs[row.type].append(
                        {
                            "type": "datatype_check",
                            "columns": [row.column_name],
                            "datatype": row.expected_datatype
                        }
                    )
            elif row.rule_type_desc == 'Special Character Check':
                configs[row.type].append(
                        {
                            "type": "special_character_check",
                            "columns": [row.column_name]
                        }
                    )
            elif row.rule_type_desc == 'Date Format Check':
                configs[row.type].append(
                        {
                            "type": "date_format_check",
                            "columns": [row.column_name],
                            "format": row.expected_date_format
                        }
                    )
            elif row.rule_type_desc == 'Fixed Length Check':
                configs[row.type].append(
                        {
                            "type": "fixed_length_check",
                            "columns": [row.column_name],
                            "length": row.threshold_low
                        }
                    )
            elif row.rule_type_desc == 'Valid Values Check':
                configs[row.type].append(
                        {
                            "type": "valid_values_check",
                            "columns": [row.column_name],
                            "valid_values": row.valid_list
                        }
                    )
    return configs


