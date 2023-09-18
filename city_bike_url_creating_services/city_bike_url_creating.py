from get_data_services.get_data_services import Step_fetching_file,File_properties,Utils_methods,Steps_fetching_files


def url_creating_citi_bike(years_list:[int],interval_month:(int,int)):
    start_month= interval_month[0]
    end_month_include= interval_month[1]
    step_fetch_list = []
    utils = Utils_methods()
    for year in years_list:
        while end_month_include >= start_month:
            #https://s3.amazonaws.com/tripdata/201309-citibike-tripdata.zip
            url_file = f'https://s3.amazonaws.com/tripdata/{year}{start_month:02}-citibike-tripdata.zip'
            file_name = f'{year}{start_month:02}-citibike-tripdata'
            file_extension= 'csv'
            file_zip_suffix = 'zip'
            file_props = File_properties(url_file,file_name,file_extension,file_zip_suffix,year,start_month)
            step_fetch = Step_fetching_file(file_props,utils)
            step_fetch_list.append(step_fetch)
            start_month += 1

    steps_object = Steps_fetching_files(step_fetch_list)
    steps_object.run_steps_fetching()