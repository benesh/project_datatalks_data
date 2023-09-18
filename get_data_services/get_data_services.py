from urllib import request
import os
import shutil
from dataclasses import dataclass
from zipfile import ZipFile
from get_data_services.settings import WORDIR_DATA_ZIP_PATH,WORDIR_DATA_EXTRACTED_PATH

@dataclass
class File_properties:
    url_file: str
    file_name : str
    file_extension : str
    file_zip_suffix : str
    year : int
    month : str

    def get_zip_directory(self) -> str:
        return f'{WORDIR_DATA_ZIP_PATH}/{self.year}/{self.month:02}'
    def get_extracted_directory(self) -> str:
        return f'{WORDIR_DATA_EXTRACTED_PATH}/{self.year}/{self.month:02}'
    def get_file_name_all(self)-> str:
        return f'{self.file_name}.{self.file_extension}.{self.file_zip_suffix}'
    def get_file_name_with_no_zip(self) -> str:
        return f'{self.file_name}.{self.file_extension}'
    def get_file_name_with_extension_zip(self) -> str:
        return f'{self.file_name}.{self.file_zip_suffix}'


class Utils_methods:
    @classmethod
    def fetching_file_with_wget(self,url_file, directory_file):
        request.urlretrieve(url_file, directory_file)
    @classmethod
    def create_diretory(self,local_path_data):
        os.makedirs(local_path_data,exist_ok=True)
    @classmethod
    def unzip_file_csv(self,file_zip_path,file_to_extract_name,extraction_ditrectory) -> None:
        shutil.unpack_archive(file_zip_path,extraction_ditrectory)
        #with ZipFile(f'{file_zip_path}', 'r') as zObject:
            #zObject.extract(file_to_extract_name, path=extraction_ditrectory)

@dataclass
class Step_fetching_file:
    file_prop : File_properties
    utils_run : Utils_methods
    def step_fetch_file_from_web(self) -> None:
        """Creating folder of zip data"""
        self.utils_run.create_diretory(self.file_prop.get_zip_directory())
        """fetching file"""
        self.utils_run.fetching_file_with_wget(self.file_prop.url_file,f"{self.file_prop.get_zip_directory()}/{self.file_prop.get_file_name_all()}")
        """Creating folder of zip data"""
        self.utils_run.create_diretory(self.file_prop.get_extracted_directory())
        """unzip file """
        self.utils_run.unzip_file_csv(file_zip_path=f'{self.file_prop.get_zip_directory()}/{self.file_prop.get_file_name_all()}',
                                      file_to_extract_name=self.file_prop.get_file_name_with_no_zip(),
                                      extraction_ditrectory=self.file_prop.get_extracted_directory())
@dataclass
class Steps_fetching_files:
    steps_to_run : [Step_fetching_file]
    def get_steps(self):
        return self.steps_to_run
    def run_steps_fetching(self) -> None:
        for step in self.steps_to_run:
            step.step_fetch_file_from_web()








