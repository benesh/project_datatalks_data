from prefect import flow
from prefect import task
from prefect_flows_2.classes_models_services.files_info_classes import FileProperties,StatusFile
import os
import logging
import pandas as pd

# adding Folder_2 to the system path

@task(name="READING FILES")
def reading_file_from_csv(file_path:str) -> pd.DataFrame:
    df_pandas = pd.read_csv(file_path)
    return df_pandas
@task
def cast_column_to_datetime(df:pd.DataFrame, columns : list) -> pd.DataFrame:
    for column in columns:
        df[column]= pd.to_datetime(df[column])
    return df
@task(name="TRANSFORM DATAFRAME")
def renaming_columns(df: pd.DataFrame, schema_redifining : dict) -> pd.DataFrame :
    df = df.rename(columns=schema_redifining)
    return df
@task(name="DROPING COLUMNS")
def droping_columns(df: pd.DataFrame,columns : list) -> pd.DataFrame :
    df = df.drop(columns)
    return df
@task(name="WRINTING INTO PARQUET FORMAT")
def writing_datafram_into_parquets(df:pd.DataFrame,file_prop: FileProperties):
    os.makedirs(file_prop.local_path_parquet)
    df.to_parquet(f'{file_prop.local_path_parquet}/{file_prop.file_name}.parquet')
@flow(name="DATA_TRANFORM_CITY_BIKE")
def data_transfrom_city_bike_flow(file_prop: FileProperties, columns_time_to_cast, schema_redifining):
    file_path_unpacked = f'{file_prop.local_path_extracted}/{file_prop.file_name}.{file_prop.file_extension_decompressed}'
    df = reading_file_from_csv( file_path_unpacked)
    df = renaming_columns(df, file_prop.columns_to_renanmes)
    df = cast_column_to_datetime(df, columns_time_to_cast)
    #df = droping_columns(df,columns_to_drop)
    writing_datafram_into_parquets(df, file_prop)
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

dtypes_2013_2020 = {

}


if __name__=='__main__':
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
    year = 2013
    month = 7
    #url_file, file_name, file_extension_c, file_extension_d, year, month
    columns_time = ['started_at','ended_at']

    collumns_to_dropt = ['usertype','tripduration']
    url_file ="wwwjilk"
    file_name = f"{year}-{month:02}-citibike-tripdata"
    file_extension_c ='zip'
    file_extension_d ='csv'

                #FileProperties(url_file,file_name,file_extension_c,file_extension_d,year,start_month,columns_to_renanmes,columns_to_cast_as_times)
    file1_prop = FileProperties(url_file,file_name,file_extension_c,file_extension_d,year,f'{month:02}',schema_redefine,columns_time)

    data_transfrom_city_bike_flow(file1_prop,columns_time,get_columns_renamed())

    logging.basicConfig(filename='/home/benomar/workspace/project_datatalks_data/prefect_flows_2/example.log', encoding='utf-8', level=logging.DEBUG)
    logging.debug('This message should go to the log file')
    logging.info('So should this')
    logging.warning('And this, too')
    logging.error('And non-ASCII stuff, too, like Øresund and Malmö')
