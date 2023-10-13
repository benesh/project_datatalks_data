from prefect_flows_part.get_data_services.task_get_data_services import run_steps_fetching
from prefect_flows_part.classes_models_services.files_info_classes import FileProperties
from prefect import flow
import sys
from urllib import request
import http.client
from urllib.request import urlopen
import json

#Replace "/path/to/your/module" with the exact path of your module directory
sys.path.append("/home/benomar/workspace/project_datatalks_data/prefect_flows_part/classes_models_services")
sys.path.append("/home/benomar/workspace/project_datatalks_data/prefect_flows_part/get_data_services")
sys.path.append("/home/benomar/workspace/project_datatalks_data/prefect_flows_part/data_transform_serices")

@flow(name="LANCHING NY BIKE FILES FETCHING")
def url_creating_citi_bike(years_list,interval_month):
    print(years_list,interval_month)
    start_month= interval_month[0]
    end_month_include= interval_month[1]
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
            file_props.append(FileProperties(url_file,file_name,file_extension_c,file_extension_d,year, start_month))
            start_month += 1
    run_steps_fetching(file_props)


if __name__=='__main__':

    year = [2013]
    month = (7,9)
    url_creating_citi_bike(year, month)
