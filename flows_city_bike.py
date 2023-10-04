from get_data_services.flow_task_get_data_services import run_steps_fetching
from classes_models_services.files_info_classes import File_properties
from prefect import flow

@flow(name="LANCHING NY BIKE FILES FETCHING ")
def url_creating_citi_bike(years_list,interval_month):
    print(years_list,interval_month)
    start_month= interval_month[0]
    end_month_include= interval_month[1]
    file_props = []
    for year in years_list:
        while end_month_include >= start_month:
            #https://s3.amazonaws.com/tripdata/201309-citibike-tripdata.zip
            if year < 2017 :
                if start_month not in (6,7) and year != 2022:
                    url_file = f'https://s3.amazonaws.com/tripdata/{year}{start_month:02}-citbike-tripdata.zip'
                else :
                    url_file = f'https://s3.amazonaws.com/tripdata/{year}{start_month:02}-citibike-tripdata.zip'
            else:
                url_file = f'https://s3.amazonaws.com/tripdata/{year}{start_month:02}-citibike-tripdata.csv.zip'
            file_name = f'{year}{start_month:02}-citibike-tripdata'
            file_extension= 'csv'
            file_zip_suffix = 'zip'
            file_props.append( File_properties(url_file,file_name,file_extension,file_zip_suffix,year,start_month) )
            start_month += 1

    run_steps_fetching(file_props)

if __name__=='__main__':
    year = [2013]
    month = (7,9)
    url_creating_citi_bike(year, month)
