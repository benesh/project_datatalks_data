from pydantic.dataclasses import dataclass
from data_settings import DataStatus

@dataclass
class DataProperties:
    url_file: str
    file_name : str
    year:int
    month:int
    file_extension_compressed : str
    file_extension_decompressed : str
    columns_to_cast_as_times : str
    columns_to_renanmes : str
    local_path_extracted : str = f'{year}/{month:02}'
    local_path_compressed : str = f'{year}/{month:02}'
    local_path_parquet : str = f'{year}/{month:02}'
    status : str = DataStatus.TO_DOWNLOAD
    cleaning_status : bool = True
