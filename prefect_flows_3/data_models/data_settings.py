from enum import Enum


class DataStatus(Enum):
    TO_DOWNLOAD = 1
    ERROR_DOWNLODING = 2
    DOWNLAODED = 3
    UNPACKED = 4
    ERROR_UNPACKING = 5
    ERROR_CLEANING = 6
    RENAMED = 7
    PQ_CREATED = 8
    ERROR_PQ = 9


