from data_transform_serices.task_for_converting_csv_to_parquet import *
from prefect import flow
from pyspark.sql import SparkSession, types
import sys

# adding Folder_2 to the system path
sys.path.insert(0, '/home/benomar/workspace/project_datatalks_data/data_transform_serices')

@flow(name="DATA_TRANFORM_CITY_BIKE")
def data_transfrom_city_bike_flow(spark, source_file, destination_file, schema_2013,
                                  schema_redifining, columns_to_drop):
    df = reading_file_from_csv(spark, source_file, schema_2013)
    df = droping_columns(df,columns_to_drop)
    df = redefining_columns_schema(df,schema_redifining)
    writing_datafram_into_parquets(df,destination_file)

if __name__=='__main__':
    spark = SparkSession.builder \
        .master("spark://5888382e901b:7077") \
        .appName("City_bike")\
        .getOrCreate()

    file_path= "/home/benomar/datawarehouse/csv/2013/07/*"
    destiation="/home/benomar/datawarehouse/pq/2013/07/"
    schema_2013_2020 = types.StructType([
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
        types.StructField('gender', types.StringType(), True)
    ])
    schema_redefine = {
                            "starttime": "started_at",
                            "stoptime": "ended_at",
                            "start station id": "start_station_id",
                            "start station name":"start_station_name",
                            "start station latitude":"start_lat",
                            "start station longitude":"start_lng",
                            "end station id":"end_station_id",
                            "end station name" :"end_station_name",
                            "end station latitude":"end_lat",
                            "end station longitude":"end_lng"
                        }
    collumns_to_dropt = ['usertype','tripduration']

    data_transfrom_city_bike_flow(spark, file_path,destiation,schema_2013_2020,
                                  schema_redefine,collumns_to_dropt)

