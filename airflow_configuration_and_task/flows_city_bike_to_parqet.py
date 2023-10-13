from prefect import flow
from pyspark.sql import SparkSession, types
import sys
from prefect import task
from pyspark.sql import  DataFrame
import pandas as pd

# adding Folder_2 to the system path
sys.path.insert(0, '/home/benomar/workspace/project_datatalks_data/data_transform_serices')

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

@flow(name="DATA_TRANFORM_CITY_BIKE")
def data_transfrom_city_bike_flow(spark, source_file, destination_file, schema_2013,
                                  schema_redifining, columns_to_drop):
    df = reading_file_from_csv(spark, source_file, schema_2013)
    df = droping_columns(df,columns_to_drop)
    df = redefining_columns_schema(df,schema_redifining)
    writing_datafram_into_parquets(df,destination_file)

def get_columns_renamed():
    schema_redefine = {
        "starttime": "started_at",
        "stoptime": "ended_at",
        "start station id": "start_station_id",
        "start station name": "start_station_name",
        "start station latitude": "start_lat",
        "start station longitude": "start_lng",
        "end station id": "end_station_id",
        "end station name": "end_station_name",
        "end station latitude": "end_lat",
        "end station longitude": "end_lng"
    }
    return schema_redefine

def schema_difinition(year:int):
    if year < 2021 :
        return  (
            types.StructType([
                types.StructField('tripduration', types.StringType(), True),
                types.StructField('starttime', types.TimestampType(), True),
                types.StructField('stoptime', types.TimestampType(), True),
                types.StructField('start station id', types.StringType(), True),
                types.StructField('start station name', types.StringType(), True),
                types.StructField('start station latitude', types.DoubleType(), True),
                types.StructField('start station longitude', types.DoubleType(), True),
                types.StructField('end station id', types.StringType(), True),
                types.StructField('end station name', types.StringType(), True),
                types.StructField('end station latitude', types.DoubleType(), True),
                types.StructField('end station longitude', types.DoubleType(), True),
                types.StructField('bikeid', types.StringType(), True),
                types.StructField('usertype', types.StringType(), True),
                types.StructField('birth year', types.StringType(), True),
                types.StructField('gender', types.StringType(), True) ])
        )
    else:
        return (
            types.StructType([
                types.StructField('ride_id', types.StringType(), True),
                types.StructField('rideable_type', types.StringType(), True),
                types.StructField('started_at', types.TimestampType(), True),
                types.StructField('ended_at', types.TimestampType(), True),
                types.StructField('start_station_name', types.StringType(), True),
                types.StructField('start_station_id', types.StringType(), True),
                types.StructField('end_station_name', types.StringType(), True),
                types.StructField('end_station_id', types.StringType(), True),
                types.StructField('start_lat', types.DoubleType(), True),
                types.StructField('start_lng', types.DoubleType(), True),
                types.StructField('end_lat', types.DoubleType(), True),
                types.StructField('end_lng', types.DoubleType(), True),
                types.StructField('member_casual', types.StringType(), True)])
        )

if __name__=='__main__':
    spark = SparkSession.builder \
        .master("spark://5888382e901b:7077") \
        .appName("City_bike")\
        .getOrCreate()

    file_path= "/home/benomar/datawarehouse/csv/2013/07/*"
    destiation="/home/benomar/datawarehouse/pq/2013/07/"

    collumns_to_dropt = ['usertype','tripduration']


