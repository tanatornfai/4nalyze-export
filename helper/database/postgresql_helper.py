import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

import time
import pandas as pd
import numpy as np

import sqlalchemy
from helper.database.base_db_helper import base_db_helper

class postgresql_helper(base_db_helper):
    ##########################################
    # Import
    ##########################################
    def command_import(self, table, input_path, optional_arguments=None):
        
        #Parameter
        delimeter = optional_arguments.get('DELIMITER')
#         escape = optional_arguments.get('ESCAPE')
        
        command_script = ''
        if self.database_type == 'postgresql':
            command_script = f"""psql d {self.default_db} -h {self.hostname} -p {self.port} -U {self.username} -c "\copy {table} from '{input_path}' with delimiter '{delimeter}' escape '\\' CSV header;" """
            
        return command_script
    
    def import_data(self, table, input_path, optional_arguments=None):
        command_script = self.command_import(table, input_path, optional_arguments)
        print(f'Importing data from {input_path} to table {table}')
        print('Using this script : ')
        print(command_script)
        return self.db_host_ssh.send_command(command_script) 
    
    
    ##########################################
    # Export
    ##########################################
    def command_export(self, script, output_path, optional_arguments=None):
        
        #Parameter
        delimeter = optional_arguments.get('DELIMITER') 
        header = optional_arguments.get('HEADER',False)
        if header==True:
            header='-t'
        else:
            header=None
        script = script.replace('\n', ' ').replace('\t', ' ')
        command_script = ''
        if self.database_type == 'postgresql':
            command_script = f' psql -d {self.default_db} -h {self.hostname} -p {self.port} {header} -U {self.username} -F "{delimeter}" --no-align --pset="footer=off" -c "{script}" > {output_path} '
        return command_script
        
    def export_data(self, script, output_path, optional_arguments=None):
        command_script = self.command_export(script, output_path, optional_arguments)
        print(f'Exporting data using {script} to {output_path}')
        print('Using this script : ')
        print(command_script)
        return self.db_host_ssh.send_command(command_script) 