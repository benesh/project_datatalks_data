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
        if self.year < 2017:
            return f'{self.file_name}.{self.file_extension}.{self.file_zip_suffix}'
        else :
            return f'{self.file_name}.{self.file_extension}.{self.file_extension}.{self.file_zip_suffix}'

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
    def unzip_file_csv(self,file_zip_path,extraction_ditrectory) -> None:
        shutil.unpack_archive(file_zip_path,extraction_ditrectory)
    @classmethod
    def verify_if_other_elem_exist(self,path_data) -> list :
        files = os.listdir(path_data)
    @classmethod
    def list_of_files_and_directory(self,path) -> list:
        return os.listdir(path)

    @classmethod
    def retiring_csv_file(self, extension_to_exclude, list_files) -> list:
        return [f for f in list_files if extension_to_exclude not in f]

    @classmethod
    def get_list_elements_(self,list_files_and_directories,path)-> (list,list) :
        list_files = []
        list_directories = []
        for element in list_files_and_directories:
            if os.path.isfile(path + '/' + element) :
                list_files.append(element)
            else:
                list_directories.append(element)
        return list_files,list_directories

    @classmethod
    def remove_files(self,list_files,path):
        for element in list_files:
            os.remove(path + '/' + element)

    @classmethod
    def remove_directories(self,list_directories,path):
        for element in list_directories:
            shutil.rmtree(path + '/' + element)

@dataclass
class Step_fetching_file:
    file_prop : File_properties
    utils_run : Utils_methods
    def step_fetch_file_from_web(self) -> None:
        """Creating folder of zip data"""
        self.utils_run.create_diretory(self.file_prop.get_zip_directory())
        """Fetching file"""
        self.utils_run.fetching_file_with_wget(self.file_prop.url_file,f"{self.file_prop.get_zip_directory()}/{self.file_prop.get_file_name_all()}")
        """Creating folder of zip data"""
        self.utils_run.create_diretory(self.file_prop.get_extracted_directory())
        """ Unzip file """
        self.utils_run.unzip_file_csv( file_zip_path=
                                      f'{self.file_prop.get_zip_directory()}/{self.file_prop.get_file_name_all()}',
                                      extraction_ditrectory=self.file_prop.get_extracted_directory() )
        """ Process of cleaning the extracted directorie """
        list_of_element = self.utils_run.list_of_files_and_directory(self.file_prop.get_extracted_directory())
        if len(list_of_element) > 1 :
            list_files,list_directories = self.utils_run.get_list_elements_(list_of_element,self.file_prop.get_extracted_directory())
            list_files = self.utils_run.retiring_csv_file(extension_to_exclude=self.file_prop.file_extension,list_files=list_files)
            self.utils_run.remove_files(list_files,self.file_prop.get_extracted_directory())
            self.utils_run.remove_directories(list_directories,self.file_prop.get_extracted_directory())

    def cleaning_directories(self,path_to_cleaning,file_name_toexclude) -> None:
        pass
    def fetching_file(self,url_file,path_to_store):
        pass



@dataclass
class Steps_fetching_files:
    steps_to_run : [Step_fetching_file]
    def get_steps(self):
        return self.steps_to_run
    def run_steps_fetching(self) -> None:
        for step in self.steps_to_run:
            step.step_fetch_file_from_web()