from enum import Enum
import json
from prefect_flows_2.classes_models_services.settings import *
class StatusFile(Enum):
    TO_DOWNLOAD = 1
    ERROR_DOWNLODING = 2
    DOWNLAODED = 3
    UNPACKED = 4
    ERROR_UNPACKING = 5
    ERROR_CLEANING = 6
    RENAMED = 7
    PQ_CREATED = 8
    ERROR_PQ = 9


class FileProperties:
    def __init__(self, url_file,file_name,file_extension_c,file_extension_d,
                 year,month,columns_to_renanmes,columns_to_cast_as_times):
        self.url_file = url_file
        self.file_name = file_name
        self.year = year
        self.month = month
        self.file_extension_compressed = file_extension_c
        self.file_extension_decompressed = file_extension_d
        self.local_path_extracted = f'{DATA_DIR_EXTRACTED_PATH}/{self.year}/{self.month:02}'
        self.local_path_compressed = f'{DATA_DIR_ZIP_PATH}/{self.year}/{self.month:02}'
        self.local_path_parquet = f'{DATA_DIR_PARQUET}/{self.year}/{self.month:02}'
        self.status: StatusFile = StatusFile.TO_DOWNLOAD
        self.columns_to_cast_as_times = columns_to_cast_as_times
        self.columns_to_renanmes = columns_to_renanmes
        self.cleaning_status = False
    def set_status_file(self, param_statu : StatusFile):
        self.status = param_statu
    def set_tatus_cleaning(self,status: bool ):
        self.cleaning_status = status
    def toJSON(self):
        return {
                    "url_file" : f"{self.url_file}",
                    "file_name" : self.file_name,
                    "year" : self.year,
                    "month" : self.month,
                    "file_extension_compressed" : self.file_extension_compressed,
                    "file_extension_decompressed" : self.file_extension_decompressed,
                    "local_path_extracted" : self.local_path_extracted ,
                    "local_path_compressed" : self.local_path_compressed,
                    "local_path_parquet" : self.local_path_parquet ,
                    "status StatusFile" : self.status.name ,
                    "columns_to_cast_as_times" : self.columns_to_cast_as_times ,
                    "columns_to_renanmes" : self.columns_to_renanmes ,
                    "cleaning_status" : self.cleaning_status
                }