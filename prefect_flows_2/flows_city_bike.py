from prefect_flows_2.get_data_services.task_get_data_services import run_steps_fetching
from prefect_flows_2.classes_models_services.files_info_classes import FileProperties
from prefect import flow
import logging


@flow(name="LANCHING NY BIKE FILES FETCHING")
def url_creating_citi_bike(years_list,interval_month):
    print(years_list,interval_month)
    start_month= interval_month[0]
    end_month_include= interval_month[1]
    columns_to_renanmes = {
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
    columns_to_cast_as_times = ['started_at','ended_at']
    file_props = []
    file_extension_c = 'zip'
    file_extension_d = 'csv'
    for year in years_list:
        while end_month_include >= start_month:
            #https://s3.amazonaws.com/tripdata/201309-citibike-tripdata.zip
            #https://s3.amazonaws.com/tripdata/201701-citibike-tripdata.csv.zip
            if year < 2017 :
                url_file = f'https://s3.amazonaws.com/tripdata/{year}{start_month:02}-citibike-tripdata.zip'
            else:
                url_file = f'https://s3.amazonaws.com/tripdata/{year}{start_month:02}-citibike-tripdata.csv.zip'
            file_name= f"{year}-{start_month:02}-citibike-tripdata"
            file_props.append(FileProperties(url_file,file_name,file_extension_c,file_extension_d,year,start_month,columns_to_renanmes,columns_to_cast_as_times))
            start_month += 1
    run_steps_fetching(file_props)


if __name__=='__main__':

    year = [2013]
    month = (7,8)
    url_creating_citi_bike(year, month)

    logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)
    logging.debug('This message should go to the log file')
    logging.info('So should this')
    logging.warning('And this, too')
    logging.error('And non-ASCII stuff, too, like Øresund and Malmö')

