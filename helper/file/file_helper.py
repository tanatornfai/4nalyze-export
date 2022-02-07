import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

import json
import shutil
import glob2
# import fastparquet

import numpy as np
import pandas as pd

from pathlib import Path

from helper.file import kerberos_helper



class file_helper:
    
    def __init__(self):
        pass

    
    
    ####################################################################
    ## READ
    ####################################################################
    def read_csv(self, file_path, config_dict = dict(), print_log=True):
        if print_log is True:
            print(f'Reading csv file : {file_path}')
        if len(config_dict) > 0:
            print('read option : ')
            print(config_dict)
        delimiter = config_dict.get('delimiter', None)
        header_row = config_dict.get('header_row', 'infer')
        nrows = config_dict.get('nrows', None)
        
        df = pd.read_csv(
            file_path,
            delimiter = delimiter,
            header = header_row,
            nrows = nrows
        )
        df = df.where(pd.notnull(df), None)
        df = df.astype(object).replace(np.nan, 'None').replace({'None': None})
        return df
        
        
    def read_excel(self, file_path, config_dict = dict()):
        print(f'Reading excel file : {file_path}')
        if len(config_dict) > 0:
            print('read option : ')
            print(config_dict)
        sheet_name = config_dict.get('sheet_name', 'Sheet1')
        header_row = config_dict.get('header_row', 0)
        nrows = config_dict.get('nrows', None)
        
        df = df.read_excel(
            file_path,
            sheet_name = sheet_name,
            header = header_row,
            nrows = nrows
        )
        df = df.where(pd.notnull(df), None)
        df = df.astype(object).replace(np.nan, 'None').replace({'None': None})
        return df
        
        
    def read_json(self, file_path, config_dict = dict()):
        pass
        
        
    def read_parquet(self, file_path, config_dict = dict()):
        pass
    
    
    def read_text(self, file_path, config_dict = dict()):
        print(f'Reading text file : {file_path}')
        if len(config_dict) > 0:
            print('read option : ')
            print(config_dict)
        encoding = config_dict.get('encoding', 'UTF-8')
        
        with open(file_path, encoding=encoding) as file:
            text = file.read().strip()
        return text
   

    def read_lines(self, file_path, config_dict = dict()):
        text = self.read_text(file_path, config_dict)
        nrows = config_dict.get('nrows', None)
        
        lines = text.split('\n')
        lines = [line.rstrip() for line in lines]
        if nrow is not None:
            lines = lines[:nrows]
        return lines
   
    
    def read_file(self, file_path, config_dict = dict(), print_log=True):
        if file_path.endswith('.csv'):
            content = self.read_csv(file_path, config_dict, print_log=print_log)
            
        elif file_path.endswith('.excel'):
            content = self.read_excel(file_path, config_dict)
            
        elif file_path.endswith('.json'):
            content = self.read_json(file_path, config_dict)
            
        elif file_path.endswith('.parquet'):
            content = self.read_parquet(file_path, config_dict)
            
        else:
            content = self.read_text(file_path, config_dict)
        
        return content
        
    
    ####################################################################
    ## GET and LIST
    ####################################################################
    def list_file(self, file_path, show_log = True, include = 'file'):
        print(f'Listing file : ')
        print(f' - File_path : {file_path}')
        file_list = sorted(list(set(glob2.glob(file_path + '**', recursive=True) + glob2.glob(file_path + '/**', recursive=True))))
        
        if include == 'file':
            file_list = [path for path in file_list if self.check_file_exists(path)]
        elif include == 'dir':
            file_list = [path for path in file_list if os.path.exists(path)]
        
        if show_log:
            print(f' - Count : {len(file_list)} files')
            for file in file_list:
                print(f'\t{file}')
        return file_list
    
    
    ####################################################################
    ## MANAGE - DELETE, MOVE, COPY
    ####################################################################
    def delete_file(self, file_path, show_log = True):
        print(f'Deleting local path :')
        print(f' - Path : {file_path}')
        
        try:
            shutil.rmtree(file_path)
        except Exception as e:
            pass
        
    def download_file(self, source_path, destination_path, clear_destination = True, show_log = True):
        self.copy_file(source_path, destination_path, clear_destination, show_log)
    
    def move_file(self, source_path, destination_path, clear_destination = True, show_log = True):
        self.copy_file(source_path, destination_path, clear_destination, show_log)
        self.delete_file(source_path, show_log = True)
    
    
    def copy_file(self, source_path, destination_path, clear_destination = True, show_log = True):
        if clear_destination:
            print(f'Clearing destination : ')
            self.delete_file(destination_path, show_log = True)
            print()
        if not self.check_file_exists(source_path):
            # Check Source Path
            print(f'Source path : ')
            file_list = self.list_file(source_path, show_log)
            print()

            # Copy check if file
            print('Copying File : ')
            print(f' - From {source_path}')
            print(f' - To {destination_path}')
            shutil.copytree(source_path, destination_path)
            print()

            # Check Destination Path
            print(f'Destination path : ')
            file_list = self.list_file(destination_path, show_log)
            print()
        else :
            print(f'Source path : {source_path}')
            self.ensure_dir(destination_path)
            print()
            
            # Copy check if file
            print('Copying File : ')
            print(f' - From {source_path}')
            print(f' - To {destination_path}')
            shutil.copyfile(source_path,destination_path)
            print()

            # Check Destination Path
            print(f'Destination path : ')
            file_list = self.list_file(destination_path, show_log)
            print()
    

    ####################################################################
    ## UTILITY
    ####################################################################
    def get_file_size(self, file_path):
        pass
        
        
    def check_file_exists(self, file_path):
        return os.path.isfile(file_path)
        
        
    def ensure_dir(self, file_path, show_log = True):
        dir_path = os.path.dirname(file_path)
        if not os.path.exists(dir_path):
            self.create_dir(dir_path, show_log)
        
        
    def create_dir(self, dir_path, show_log = True):
        print(f'Creating dir : {dir_path}')
        Path(dir_path).mkdir(parents=True, exist_ok=True)
     