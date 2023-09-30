from urllib import request
import os
import shutil
from dataclasses import dataclass

from prefect import flow,task


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


@task(name="Fetching File from Web")
def fetching_file_with_wget(url_file, directory_file):
    request.urlretrieve(url_file, directory_file)

@task(name="Creation of Fdolders")
def create_diretory(local_path_data):
    os.makedirs(local_path_data,exist_ok=True)

@task(name="Unziping File")
def unzip_file_csv(file_zip_path,extraction_ditrectory) -> None:
    shutil.unpack_archive(file_zip_path,extraction_ditrectory)

@task(name="VERIFYING IF OTHERS FILE EXIST")
def verify_if_other_elem_exist(path_data) -> list :
    files = os.listdir(path_data)

@task(name="LIST OF ELEMENTS")
def list_of_files_and_directory(path) -> list:
    return os.listdir(path)

@task(name="")
def retiring_csv_file(extension_to_exclude, list_files) -> list:
    return [f for f in list_files if extension_to_exclude not in f]

@task(name="SEPARATION OF FILES AND FOLDERS")
def get_list_elements_(list_files_and_directories,path)-> (list,list) :
    list_files = []
    list_directories = []
    for element in list_files_and_directories:
        if os.path.isfile(path + '/' + element) :
            list_files.append(element)
        else:
            list_directories.append(element)
    return list_files,list_directories

@task(name="DELETING FILES")
def remove_files(list_files,path):
    for element in list_files:
        os.remove(path + '/' + element)

@task(name="DELETING FOLDER")
def remove_directories(list_directories,path):
    for element in list_directories:
        shutil.rmtree(path + '/' + element)

@flow(name="SUBFLOW FETCH UNZIP AND CLEAN FILE")
def step_fetch_file_from_web(file_prop : File_properties) -> None:
    """Creating folder of zip data"""
    create_diretory(file_prop.get_zip_directory())
    """Fetching file"""
    fetching_file_with_wget(file_prop.url_file,f"{file_prop.get_zip_directory()}/{file_prop.get_file_name_all()}")
    """Creating folder of zip data"""
    create_diretory(file_prop.get_extracted_directory())
    """ Unzip file """
    unzip_file_csv( file_zip_path=
                                  f'{file_prop.get_zip_directory()}/{file_prop.get_file_name_all()}',
                                  extraction_ditrectory=file_prop.get_extracted_directory() )
    """ Process of cleaning the extracted directorie """
    list_of_element = list_of_files_and_directory(file_prop.get_extracted_directory())
    if len(list_of_element) > 1 :
        list_files,list_directories = get_list_elements_(list_of_element,file_prop.get_extracted_directory())
        list_files = retiring_csv_file(extension_to_exclude=file_prop.file_extension,list_files=list_files)
        remove_files(list_files,file_prop.get_extracted_directory())
        remove_directories(list_directories,file_prop.get_extracted_directory())

@task(name="CLEAN DIRECTORIES")
def cleaning_directories(path_to_cleaning,file_name_toexclude) -> None:
    pass

def fetching_file(url_file,path_to_store):
        pass

@ flow(name="SUBFLOW LIST FLOWS FETCH FILE FROM WEB")
def run_steps_fetching(file_props) -> None:
    for file_prop in file_props:
        step_fetch_file_from_web(file_prop)