from city_bike_url_creating_services import city_bike_url_creating

url_file ='https://s3.amazonaws.com/tripdata/201309-citibike-tripdata.zip'
directory_file='/home/benomar/datawarehouse/zip_data/2013/01/201309-citibike-tripdata.zip'

if __name__=='__main__':
    year = [2023]
    month = (5,5)
    city_bike_url_creating.url_creating_citi_bike(year,month)

