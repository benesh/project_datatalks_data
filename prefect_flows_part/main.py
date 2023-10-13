from prefect import flow
from flows_city_bike import url_creating_citi_bike
from enum import Enum
import os
@flow()
def start_flow_in_main():
    year = [2023]
    month = (4, 4)
    url_creating_citi_bike(year, month)


class Status_File(Enum):
    TO_DOWLOAD = 1
    DOWLAODED = 2
    UNCOMPRESSED = 3
    PQ_CREATED = 4

class Student:
    # Class variable

    def __init__(self, name, roll_no):
        self.name = name
        self.roll_no = roll_no
        self.school_name = name


if __name__=='__main__':
    temps_stat = Status_File.TO_DOWLOAD
    print(temps_stat.TO_DOWLOAD)

    # create first object
    s1 = Student('Emma', 10)
    print(s1.name, s1.roll_no, s1.school_name)
    # access class variable

    # create second object
    s2 = Student('Jessa', 20)
    # access class variable
    print(s2.name, s2.roll_no, s2.school_name)

    print(os.listdir("/home/benomar/moppo"))

