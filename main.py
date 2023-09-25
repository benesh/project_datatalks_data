import os
import shutil


def get_list_elements_( list_files_and_directories, path) -> (list, list):
    list_files = []
    list_directories = []
    for element in list_files_and_directories:
        if os.path.isfile(path + '/' + element):
            list_files.append(element)
        else:
            list_directories.append(element)
    return list_files, list_directories

def retiring_csv_file(extension_to_exclude, list_files) -> list:
    return [f for f in list_files if extension_to_exclude not in f]

from city_bike_url_creating_services import city_bike_url_creating
from urllib import request

url_file ='https://s3.amazonaws.com/tripdata/201309-citibike-tripdata.zip'
directory_file='/home/benomar/datawarehouse/zip_data/2013/01/201309-citibike-tripdata.zip'

if __name__=='__main__':
    year = [2023]
    month = (5,5)
    city_bike_url_creating.url_creating_citi_bike(year,month)
    """
    list_element = os.listdir('/home/benomar/datawarehouse/csv/2023/01')
    list_files,list_directories = get_list_elements_(list_element,'/home/benomar/datawarehouse/csv/2023/01')

    print(list_files)
    list_files= retiring_csv_file('csv',list_files)
    print(list_files)
    print('directories',list_directories)
    """


