import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

import time
import pandas as pd
import numpy as np

import sqlalchemy
from helper.database.base_db_helper import base_db_helper

class mssql_helper(base_db_helper):
    ##########################################
    # Import
    ##########################################
    def command_import(self, table, input_path, optional_arguments):
        
        #Parameter
        delimiter = optional_arguments.get('DELIMITER', '|')
        hostname = optional_arguments.get('HOSTNAME', None)
        other_parameters = optional_arguments.get('OTHER_PARAMETERS', None)
        
        if hostname is None:
            command_script = f'bcp "{table}" in {input_path} -c -T -t"{delimiter}"'
        elif hostname is not None:
            command_script = f'bcp "{table}" in {input_path} -c -t"{delimiter}" -S {hostname} -U {self.username}'
        
        if other_parameters is not None:
            command_script = f'{command_script} {other_parameters}'
        return command_script
    
    def import_data(self, table, input_path, optional_arguments=dict(), print_log=True, return_log=False):
        hostname = optional_arguments.get('HOSTNAME', None)
        password = None if (hostname is None) else self.password
        
        command_script = self.command_import(table, input_path, optional_arguments)
        if print_log is True:
            print(f'Importing data from {input_path} to table {table}')
            print('\tUsing this script : ')
            print(command_script)
        
        if return_log is True:
            log = ''
            log += f'Importing data from {input_path} to table {table}\n'
            log += '\tUsing this script : \n'
            log += f'{command_script}\n'
            return_code, stdout, stderr, send_command_log = self.db_host_ssh.send_command(command_script, 
                                                                                          print_log=print_log, 
                                                                                          return_log=return_log,
                                                                                          password=password)
            log += send_command_log
            return return_code, stdout, stderr, log
        
        return self.db_host_ssh.send_command(command_script, password=password)
    
    ##########################################
    # Export
    ##########################################
    def command_export(self, script, output_path, optional_arguments, start_row=None, end_row=None):
        
        #Parameter
        delimiter = optional_arguments.get('DELIMITER', '|')
        hostname = optional_arguments.get('HOSTNAME', None)
        
        script = script.replace('\n', ' ').replace('\t', ' ')
        if hostname is None:
            command_script = f'bcp "{script}" queryout {output_path} -c -T -t"{delimiter}"'
        elif hostname is not None:
            command_script = f'bcp "{script}" queryout {output_path} -c -t"{delimiter}" -S {hostname} -U {self.username}'
        
        if start_row is not None:
            command_script = f"{command_script} -F {start_row}"
        if end_row is not None:
            command_script = f"{command_script} -L {end_row}"
            
        return command_script
        
    def export_data(self, script, output_path, optional_arguments=dict(), print_log=True, return_log=False, start_row=None, end_row=None):
        hostname = optional_arguments.get('HOSTNAME', None)
        password = None if (hostname is None) else self.password
        
        command_script = self.command_export(script, output_path, optional_arguments, start_row=start_row, end_row=end_row)
        if print_log is True:
            print(f'Exporting data using {script} to {output_path}')
            print('\tUsing this script : ')
            print(command_script)
            
        if return_log is True:
            log = ''
            log += f'Exporting data using {script} to {output_path}\n'
            log += '\tUsing this script : \n'
            log += f'{command_script}\n'
            return_code, stdout, stderr, send_command_log = self.db_host_ssh.send_command(command_script, 
                                                                                          print_log=print_log, 
                                                                                          return_log=return_log,
                                                                                          password=password)
            log += send_command_log
            return return_code, stdout, stderr, log
        
        return self.db_host_ssh.send_command(command_script, password=password)
    
    ###########################################
    # Fetch
    ###########################################
    def generate_sql_fetch(self, script, row_start=None, row_end=None):
        
        sql_script = ''
        if (row_start is not None) and (row_end is not None) and (int(row_end) > int(row_start)):
            script = script.strip(';')
            index1 = row_start - 1
            index2 = row_end - row_start + 1
            sql_script = f'{script} offset {index1} rows fetch next {index2} rows only;'
        else:
            sql_script = script
            
        return sql_script