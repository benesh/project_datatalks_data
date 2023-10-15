from enum import Enum
from prefect_flows_2.classes_models_services.settings import *
class StatusFile(Enum):
    TO_DOWNLOAD = 1
    ERROR_DOWNLODING = 2
    DOWNLAODED = 3
    UNPACKED = 4
    ERROR_UNPACKING = 5
    PQ_CREATED = 6
    ERROR_PQ = 7
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
    def setStatusTo(self,param_statu : StatusFile):
        self.status = param_statu