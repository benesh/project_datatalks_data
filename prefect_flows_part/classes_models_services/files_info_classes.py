from enum import Enum
from prefect_flows_part.classes_models_services.settings import *

class StatusFile(Enum):
    TO_DOWNLOAD = 1
    DOWNLAODED = 2
    UNPACKED = 3
    PQ_CREATED = 4


class FileProperties:
    def __init__(self, url_file,file_name,file_extension_c,file_extension_d,year,month):
        self.url_file = url_file
        self.file_extension_compressed = file_extension_c
        self.file_extension_decompressed = file_extension_d
        self.year = year
        self.month = month
        self.local_path_extracted = f'{DATA_DIR_EXTRACTED_PATH}/{self.year}/{self.month:02}'
        self.local_path_compressed = f'{DATA_DIR_ZIP_PATH}/{self.year}/{self.month:02}'
        self.local_path_parquet = f'{DATA_DIR_PARQUET}/{self.year}/{self.month:02}'
        self.file_name = file_name
        self.status: StatusFile = StatusFile.TO_DOWNLOAD
        self.columns_times = None

    def setStatusTo(self,statu : StatusFile):
        self.status = StatusFile
    def get_compressed_folder(self) -> str:
        return f'{DATA_DIR_ZIP_PATH}/{self.year}/{self.month:02}'
    def get_extracted_folder(self) -> str:
        return f'{DATA_DIR_EXTRACTED_PATH}/{self.year}/{self.month:02}'
