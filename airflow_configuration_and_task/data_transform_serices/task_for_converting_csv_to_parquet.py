from prefect import task
from pyspark.sql import DataFrame
import pandas as pd

@task(name="READING FILES")
def reading_file_from_csv(spark,file_path,schema) -> DataFrame:
    df = (spark.read
          .option("header", True)
          .schema(schema)
          .csv(file_path))
    return df
@task(name="TRANSFORM DATAFRAME")
def redefining_columns_schema(df: DataFrame, schema_redifining : dict) -> DataFrame :
    df = df.withColumnsRenamed(schema_redifining)
    return df
@task(name="DROPING COLUMNS")
def droping_columns(df: DataFrame,columns : list) -> DataFrame :
    def inner_droping_columns(df: DataFrame, column:str ) -> DataFrame :
        return df.drop(column)
    for column in columns:
        df = inner_droping_columns(df , column)
    return df
@task(name="WRINTING INTO PARQUET FORMAT")
def writing_datafram_into_parquets(df:DataFrame,destination_file: str):
    df.write.parquet(destination_file)