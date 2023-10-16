import json
import sys
from urllib import request,error
import os
import shutil
from prefect_flows_2.classes_models_services.files_info_classes import FileProperties,StatusFile
from prefect_flows_2.classes_models_services.settings import *
from prefect import task,flow
import logging
from datetime import date

sys.path.insert(0, '/home/benomar/workspace/project_datatalks_data/prefect_flows_2/classes_models_services')

def get_name_file_with_given_extension(path,extesnion) -> str :
    elements = os.listdir(path)
    for elem in  elements:
        if extesnion in elem :
            logging.info(f'Element {elements} Found')
            return elem
        else:
            logging.info("No Element Found")
@task(name="FETCHING_FILE_FROM_WEB")
def fetching_file_with_wget(file_prop: FileProperties):
    path_filename_compressed = f'{file_prop.local_path_compressed}/{file_prop.file_name}.{file_prop.file_extension_compressed}'
    try:
        """Creating folder of zip data"""
        create_diretory(file_prop.local_path_compressed)
        tempvar = request.urlretrieve(file_prop.url_file, path_filename_compressed)
        logging.info(tempvar[1])
        file_prop.set_status_file(StatusFile.DOWNLAODED)
        logging.info(f"Fetching File {file_prop.file_name} in the driectory {path_filename_compressed}")
    except Exception as e:
        file_prop.set_status_file(StatusFile.ERROR_DOWNLODING)
        logging.exception(e)
def create_diretory(local_path_data) -> None :
    os.makedirs(local_path_data,exist_ok = True)
    logging.info(f"CREATION OF FOLDER {local_path_data}")

@task(name="DECOMPRESS_FILE")
def unzip_file_csv(file_prop : FileProperties) -> None:
    path_compressed_file = f'{file_prop.local_path_compressed}/{file_prop.file_name}.{file_prop.file_extension_compressed}'
    path_extracted_file = f'{file_prop.local_path_extracted}'
    try :
        create_diretory(file_prop.local_path_extracted)
        shutil.unpack_archive(path_compressed_file,path_extracted_file)
        logging.info(f"File {path_compressed_file} unpacked")
    except Exception as e :
        file_prop.set_status_file(StatusFile.ERROR_UNPACKING)
        logging.exception(f'Unpacking {file_prop.local_path_compressed}/{file_prop.file_name}.{file_prop.file_extension_decompressed}: {e}')
@task(name="VERIFYING_IF_OTHERS FILE EXIST")
def verify_if_other_elem_exist(path_data) -> list :
    list_elements = os.listdir(path_data)
    return list_elements

def list_of_elements(path) -> list:
    logging.info("LIST OF ELEMENTS")
    return os.listdir(path)
def exclude_files_with_extension(extension_to_exclude, list_files) -> list:
    logging.info("LISTE_FILE_TO_EXCLUDE")
    return [f for f in list_files if extension_to_exclude not in f]
def get_files_folders_separate(list_files_and_directories, path)-> (list, list) :
    #result = list(filter(lambda x: (x % 13 == 0), my_list))
    list_files = []
    list_directories = []
    for element in list_files_and_directories :
        if os.path.isfile(path + '/' + element) :
            list_files.append(element)
        else:
            list_directories.append(element)
    logging.info("SEPARATION_OF_FILES_AND_FOLDERS")
    return list_files,list_directories
def remove_files(list_files,path):
    for element in list_files:
        os.remove(path + '/' + element)
    logging.info("DELETING_FILES")
@task(name="Renaming File")
def rename_file(src,dest):
    shutil.move(src,dest)
    logging.info(f'File {src} renamed to {dest}')
@task(name="DELETING FOLDER")
def remove_directories(list_directories,path):
    for element in list_directories:
        shutil.rmtree(path + '/' + element)
        logging.info(f"DELETING FOLDER {path}/{element}")

@flow(name="FETCH_UNZIP_AND_CLEAN_FILE")
def step_fetch_file_from_web(file_prop : FileProperties) -> None:
    """Fetching file"""
    fetching_file_with_wget(file_prop)
    """Unzip file"""
    unzip_file_csv(file_prop)
    """Process of cleaning the extracted directorie and renaming File"""
    cleaning_directories(file_prop)
    """Renaming file"""
    file_name = get_name_file_with_given_extension(file_prop.local_path_extracted,
                                                   file_prop.file_extension_decompressed)
    rename_file(f'{file_prop.local_path_extracted}/{file_name}',
                f'{file_prop.local_path_extracted}/{file_prop.file_name}.{file_prop.file_extension_decompressed}')
    logging.info(f"File {file_prop.file_name} is Done ")

@task(name="CLEAN DIRECTORIES")
def cleaning_directories(file_prop:FileProperties) -> None:
    try :
        list_of_element = list_of_elements(file_prop.local_path_extracted)
        if len(list_of_element) > 1:
            list_files, list_directories= get_files_folders_separate(list_of_element, file_prop.local_path_extracted)
            if len(list_files) > 1:
                list_files= exclude_files_with_extension(file_prop.file_extension_decompressed, list_files)
                remove_files(list_files, file_prop.local_path_extracted)
            if list_directories > 0 :
                remove_directories(list_directories, file_prop.local_path_extracted)
        file_prop.set_tatus_cleaning(True)
    except Exception as e:
        logging.exception(f'Erro cleanig {file_prop.local_path_extracted}/{file_prop.file_name}.{file_prop.file_extension_decompressed} : {e}')

@flow(name="SUBFLOW LIST FLOWS FETCH FILE FROM WEB")
def run_steps_fetching(file_props) -> None:
    list_of_file_props = []
    for file_prop in file_props:
        step_fetch_file_from_web(file_prop)
        list_of_file_props.append(json.dumps(file_prop.toJSON(),indent=4))
    print(list_of_file_props)
    #get the date
    today = date.today()
    d4 = today.strftime("%b-%d-%Y")
    file_path = f'{METADATA_DIR}/{d4}_metadata_execution.json'
    # Writing to sample.json
    with open(file_path, "w") as outfile:
        outfile.write(list_of_file_props)
