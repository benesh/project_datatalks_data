from prefect import flow
from flows_city_bike import url_creating_citi_bike

@flow()
def start_flow_in_main():
    year = [2023]
    month = (4, 4)
    url_creating_citi_bike(year, month)



if __name__=='__main__':

    start_flow_in_main()

