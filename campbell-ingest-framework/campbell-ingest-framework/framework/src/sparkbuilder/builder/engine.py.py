# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# TODO: to remove when build the wheel 
import sys
sys.path.append("../..")

from sparkbuilder.transformations.sql_transformation import run_sql_transformation
from sparkbuilder.transformations.python_transformation import run_py_transformation
from sparkbuilder.transformations.common_transformations import run_where_clause, run_select_clause, run_drop_columns, run_rename_columns, run_normalize_cols
from sparkbuilder.transformations.brute_force_comparison import run_brute_force_comparison
from sparkbuilder.readers.reader import Reader
from sparkbuilder.writers.writer import Writer


class PipelineBuilder(object):

    def __init__(self, spark, config, verbose=False, fncs=[]):
        
        if not isinstance(config, dict):
            raise ValueError("config must be a dictionary")
        self.config = config
        self.spark = spark
        self.transformations = []
        c = self.config
        if "transformations" in c:
            self.transformations = c["transformations"]
            if not isinstance(self.transformations, list):
                raise ValueError("transformations must be a list")
        else:
            self.transformations = []
        self.transfomation_queue = self.transformations.copy()
        self.verbose = verbose
        self.step_counter = 0
        self.transformation_mapping = {
            "sql": run_sql_transformation,
            "py": run_py_transformation,
            "where": run_where_clause,
            "select": run_select_clause,
            "drop": run_drop_columns,
            "rename": run_rename_columns,
            "normalize_cols": run_normalize_cols,
            "brute_force_subtract": run_brute_force_comparison,
        }
        self.fncs = fncs

    
    
    def datatype_conversion(self,df):
     
        # helps to convert string data type to the data types in the config table column: cast_column 
        if len(self.config["cast_column"]) > 0:
           
            for column_name, data_type in self.config["cast_column"][0].items():
                # change column data type 
                try:
                    df = df.withColumn(column_name, F.col(column_name).cast(data_type))
                except Exception as e:
                    print(f"Exception: Datatype conversion Error in sparkbuilder.builder.engine.py => datatype_conversion(). Make sure String value conforms to the datatype: {column_name}.{data_type}")
                    print(str(e))
                    raise    
            return df    
        else:     
            # no data type conversions   
            return df
    
    def add_transformation(self, transformation):
        if not isinstance(transformation, dict):
            raise ValueError("transformation must be a dictionary")

        self.transfomation_queue.append(transformation)

    def add_transformations(self, transformations):
        if not isinstance(transformations, list):
            raise ValueError("transformations must be a list")

        self.transfomation_queue += transformations

    def __len__(self):
        return len(self.transfomation_queue)

    def __iter__(self):
        return self.transfomation_queue.__iter__()

    def read(self):
        
        r = Reader(self.spark, self.config)
        df = r.read()
        return df

    def write(self, df):
        w = Writer(self.spark, self.config)
        w.write(df)

    def step(self, df):
        if len(self.transfomation_queue) == 0:
            return df
        else:
            t = self.transfomation_queue.pop(0)
            if 'id' in t:
                step_id = t['id']
            else:
                step_id = f"step_{self.step_counter}"
            if not isinstance(t, dict):
                raise ValueError("Each transformation must be a dictionary in JSON format")
            if 'py' in t.keys():
                if 'args' in t.keys():
                    args = t['args']
                    df = run_py_transformation(df, t['py'], args=args, fncs=self.fncs)
                else:
                    df = run_py_transformation(df, t['py'], fncs=self.fncs)
            elif 'brute_force_subtract' in t.keys():
                df = run_brute_force_comparison(df, t['brute_force_subtract']["table"], t['brute_force_subtract']["pkeys"])
            elif 'normalize_cols' in t.keys():
                df = run_normalize_cols(df)
            else:
                df = self.transformation_mapping[t['type']](df, t, self.config)
        self.step_counter += 1
        return df, step_id

    def run(self, df):
        step_ids = []
        while len(self.transfomation_queue) > 0:
            df, step_id = self.step(df)
            step_ids.append(step_id)
        return df, step_ids
