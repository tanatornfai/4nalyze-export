import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

import time
import pandas as pd
import numpy as np

import sqlalchemy
from helper.database.base_db_helper import base_db_helper

class mysql_helper(base_db_helper):
    ##########################################
    # Import
    ##########################################
    def command_import(self, table, input_path, optional_arguments=None):
        
        #Parameter
        delimeter = optional_arguments.get('DELIMITER', '|')
        hostname = optional_arguments.get('HOSTNAME', None)
        other_parameters = optional_arguments.get('OTHER_PARAMETERS', '')
        
        command_script = ''
        if self.database_type == 'mysql' and hostname == 'localhost':
            command_script = f""" mysql --local-infile=1 --login-path=root -e "LOAD DATA LOCAL INFILE '{input_path}'  INTO TABLE {table} FIELDS TERMINATED BY '{delimeter}' {other_parameters}" """
        elif self.database_type == 'mysql' and hostname != 'localhost':
            command_script = f""" mysql --local-infile=1 --host={hostname} --user={self.username} --password -e "LOAD DATA LOCAL INFILE '{input_path}'  INTO TABLE {table} FIELDS TERMINATED BY '{delimeter}' {other_parameters}" """
        return command_script
    
    def import_data(self, table, input_path, optional_arguments=None, print_log=True, return_log=False):
        
        hostname = optional_arguments.get('HOSTNAME', None)
        password = None if (hostname is None) else self.password
        
        command_script = self.command_import(table, input_path, optional_arguments)
        if print_log is True:
            print(f'Importing data from {input_path} to table {table}')
            print('Using this script : ')
            print(command_script)
            
        if return_log is True:
            log = ''
            log += f'Importing data from {input_path} to table {table}\n'
            log += 'Using this script : \n'
            log += f'{command_script}\n'
            
            return_code, stdout, stderr, send_command_log = self.db_host_ssh.send_command(command_script, 
                                                                                          print_log=print_log, 
                                                                                          return_log=return_log,
                                                                                          password=password)
            log += send_command_log
            return return_code, stdout, stderr, log
        
        return self.db_host_ssh.send_command(command_script)
    
    
    ##########################################
    # Export
    ##########################################
    def command_export(self, script, output_path, optional_arguments=None, start_row=None, end_row=None):
        
        #Parameter
        delimeter = optional_arguments.get('DELIMITER')
        hostname = optional_arguments.get('HOSTNAME')
        
        delimeter = f"'s/\\t/{delimeter}/g'"
        script = script.replace('\n', ' ').replace('\t', ' ')
        if (start_row is not None) and (end_row is not None):
            script = self.generate_sql_fetch(script, start_row, end_row)
        
        command_script = ''
        if self.database_type == 'mysql' and hostname == 'localhost':
            command_script = f'mysql --login-path=root --skip-column-names -e "{script}" | sed {delimeter} {other_parameters} > {output_path}'
        elif self.database_type == 'mysql'and hostname != 'localhost':
            command_script = f'mysql --host={hostname} --user={self.username} --password --skip-column-names -e "{script}" | sed {delimeter} > {output_path}'
        return command_script
        
    def export_data(self, script, output_path, optional_arguments=None, print_log=True, return_log=False, start_row=None, end_row=None):
        
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

        return self.db_host_ssh.send_command(command_script)
    
    ###########################################
    # Fetch
    ###########################################
    def generate_sql_fetch(self, script, row_start=None, row_end=None):
        row_start = row_start - 1
        sql_script = ''
        if (row_start is not None) and (row_end is not None) and (int(row_end) > int(row_start)):
            sql_script = f'{script} limit {row_start},{row_end-row_start};'
        else:
            sql_script = script
            
        return sql_script