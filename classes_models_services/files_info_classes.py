from dataclasses import dataclass

WORDIR_DATA_ZIP_PATH = '/home/benomar/datawarehouse/zip_data'
WORDIR_DATA_EXTRACTED_PATH = '/home/benomar/datawarehouse/csv'



@dataclass
class File_properties:
    url_file: str
    file_name : str
    file_extension : str
    file_zip_suffix : str
    year : int
    month : int

    def get_zip_directory(self) -> str:
        return f'{WORDIR_DATA_ZIP_PATH}/{self.year}/{self.month:02}'
    def get_extracted_directory(self) -> str:
        return f'{WORDIR_DATA_EXTRACTED_PATH}/{self.year}/{self.month:02}'
    def get_file_name_all(self)-> str:
        if self.year < 2017:
            return f'{self.file_name}.{self.file_extension}.{self.file_zip_suffix}'
        else :
            return f'{self.file_name}.{self.file_extension}.{self.file_extension}.{self.file_zip_suffix}'
    def get_file_name_with_no_zip(self) -> str:
        return f'{self.file_name}.{self.file_extension}'
    def get_file_name_with_extension_zip(self) -> str:
        return f'{self.file_name}.{self.file_zip_suffix}'
