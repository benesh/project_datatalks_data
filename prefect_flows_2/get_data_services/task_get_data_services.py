import sys
from urllib import request,error
import os
import shutil
from prefect_flows_2.classes_models_services.files_info_classes import FileProperties,StatusFile
from prefect import task,flow
import logging

sys.path.insert(0, '/home/benomar/workspace/project_datatalks_data/prefect_flows_2/classes_models_services')

@task(name="NAME_OF_FILE_DOWNLOADED")
def get_name_file_with_given_extension(path,extesnion) -> str :
    elements = os.listdir(path)
    for elem in  elements:
        if extesnion in elem :
            return elem
        else:
            logging.info("No Eelement Found")
@task(name="FETCHING_FILE_FROM_WEB")
def fetching_file_with_wget(file_prop: FileProperties):
    """Creating folder of zip data"""
    path_filename_compressed = f'{file_prop.local_path_compressed}/{file_prop.file_name}.{file_prop.file_extension_compressed}'
    try:
        create_diretory(file_prop.local_path_compressed)
        tempvar = request.urlretrieve(file_prop.url_file, path_filename_compressed)
        logging.info(tempvar[1])
        file_prop.setStatusTo(StatusFile.DOWNLAODED)
        """Creating folder of extracted file"""
    except Exception as e:
        file_prop.setStatusTo(StatusFile.ERROR_DOWNLODING)
        logging.exception(e)
@task(name="CREATION_OF_FOLDER")
def create_diretory(local_path_data) -> None :
    try:
        os.makedirs(local_path_data,exist_ok = True)
    except OSError as e :
        logging.exception(f'Directory {local_path_data} : {e}')
@task(name="DECOMPRESS_FILE")
def unzip_file_csv(path_compressed_file,path_extracted_file) -> None:
    shutil.unpack_archive(path_compressed_file,path_extracted_file)
@task(name="VERIFYING_IF_OTHERS FILE EXIST")
def verify_if_other_elem_exist(path_data) -> list :
    list_elements = os.listdir(path_data)
    return list_elements
@task(name="LIST_OF_ELEMENTS")
def list_of_elements(path) -> list:
    return os.listdir(path)
@task(name="LISTE_FILE_TO_EXCLUDE")
def retiring_csv_file(extension_to_exclude, list_files) -> list:
    return [f for f in list_files if extension_to_exclude not in f]
@task(name="SEPARATION_OF_FILES_AND_FOLDERS")
def get_files_folders_separate(list_files_and_directories, path)-> (list, list) :
    list_files = []
    list_directories = []
    for element in list_files_and_directories :
        if os.path.isfile(path + '/' + element) :
            list_files.append(element)
        else:
            list_directories.append(element)
    return list_files,list_directories
@task(name="DELETING_FILES")
def remove_files(list_files,path):
    for element in list_files:
        os.remove(path + '/' + element)
def rename_file(src,dest):
    shutil.move(src,dest)
@task(name="DELETING FOLDER")
def remove_directories(list_directories,path):
    for element in list_directories:
        shutil.rmtree(path + '/' + element)

@flow(name="FETCH_UNZIP_AND_CLEAN_FILE")
def step_fetch_file_from_web(file_prop : FileProperties) -> None:
    """Fetching file"""
    fetching_file_with_wget(file_prop.url_file)
    """Creating folder of extracted file"""
    create_diretory(file_prop.local_path_extracted)
    """Unzip file""" # revoir l'appel
    path_file_compressed = f'{file_prop.local_path_compressed}/{file_prop.file_name}.{file_prop.file_extension_compressed}'
    unzip_file_csv(path_file_compressed,file_prop.local_path_extracted)
    file_prop.setStatusTo(StatusFile.UNPACKED)
    """ Process of cleaning the extracted directorie """
    list_of_element = list_of_elements(file_prop.local_path_extracted)
    if len(list_of_element) > 1 :
        list_files,list_directories = get_files_folders_separate(list_of_element,file_prop.local_path_extracted)
        if list_files > 1 :
            list_files = retiring_csv_file(extension_to_exclude=".csv", list_files=list_files)
            remove_files(list_files, file_prop.get_extracted_folder())
        remove_directories(list_directories, file_prop.get_extracted_folder())
    """Renaming file"""
    file_name = get_name_file_with_given_extension(file_prop.local_path_extracted,
                                                   file_prop.file_extension_decompressed)
    rename_file(f'{file_prop.local_path_extracted}/{file_name}',
                f'{file_prop.local_path_extracted}/{file_prop.file_name}.{file_prop.file_extension_decompressed}')
    print(f"Done {file_prop.file_name}")
@task(name="CLEAN DIRECTORIES")
def cleaning_directories(path_to_cleaning,file_name_toexclude) -> None:
    pass
def fetching_file(url_file,path_to_store):
        pass
@flow(name="SUBFLOW LIST FLOWS FETCH FILE FROM WEB")
def run_steps_fetching(file_props) -> None:
    for file_prop in file_props:
        step_fetch_file_from_web(file_prop)