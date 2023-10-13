from prefect import flow
import sys
from prefect import task
from prefect_flows_part.classes_models_services.files_info_classes import FileProperties,StatusFile
import pandas as pd

# adding Folder_2 to the system path
sys.path.insert(0, '/home/benomar/workspace/project_datatalks_data/data_transform_serices')



@task(name="READING FILES")
def reading_file_from_csv(file_path:str) -> pd.DataFrame:
    df_pandas = pd.read_csv(file_path)
    return df_pandas
@task
def transform_column_datetime(df:pd.DataFrame, columns : list) -> pd.DataFrame:
    for column in columns:
        df[column]= pd.to_datetime(df[column])
    return df

@task(name="TRANSFORM DATAFRAME")
def redefining_columns_schema(df: pd.DataFrame, schema_redifining : dict) -> pd.DataFrame :
    df = df.renamed(columns=schema_redifining)
    return df
@task(name="DROPING COLUMNS")
def droping_columns(df: pd.DataFrame,columns : list) -> pd.DataFrame :
    df = df.drop(columns)
    return df

@task(name="WRINTING INTO PARQUET FORMAT")
def writing_datafram_into_parquets(df:pd.DataFrame,destination_file: str):
    df.write.parquet(destination_file)

@flow(name="DATA_TRANFORM_CITY_BIKE")
def data_transfrom_city_bike_flow(file_props: FileProperties,columns_time_to_cast,schema_redifining):
    file_path_unpacked = f'{file_props.local_path_extracted}/{file_props.file_name}.{file_props.local_path_extracted}'
    df = reading_file_from_csv( file_path_unpacked)
    df = transform_column_datetime(df,columns_time_to_cast)
    #df = droping_columns(df,columns_to_drop)
    df = redefining_columns_schema(df,schema_redifining)
    writing_datafram_into_parquets(df,file_props.local_path_parquet)

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

    year = 2013
    month = 2
    #url_file, file_name, file_extension_c, file_extension_d, year, month
    columns_time = ['starttime','stoptime']

    collumns_to_dropt = ['usertype','tripduration']
    url_file ="wwwjilk"
    file_name = f"{year}-{month:02}-citibike-tripdata"
    file_extension_c ='zip'
    file_extension_d ='csv'

    file1_prop = FileProperties(url_file,file_name,file_extension_c,file_extension_d,year,month)

    data_transfrom_city_bike_flow(file1_prop,columns_time,get_columns_renamed())


