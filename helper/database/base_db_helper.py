import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

import time

import pandas as pd
import numpy as np

from urllib.parse import quote  
from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker

from helper.shell.ssh_helper import ssh_helper
from helper.file import file_manager

import sqlalchemy
# import pyodbc

class base_db_helper():
    endpoint_type = 'DB'
    driver = None
    hostname = None
    username = None
    password = None
    port = None
    default_db = None
    conn = None
    engine = None
    raw_conn = None
    db_host_ssh = None
    database_type = None
    
    def __init__(self, row, endpoint_id, host_endpoint_id=None, print_log=True):
        self.create_con(row, endpoint_id, print_log=print_log)
        if host_endpoint_id != None:
            self.db_host_ssh = ssh_helper(host_endpoint_id, print_log=print_log)
            
    
    
    
    ##########################################
    # Create Connection
    ##########################################
    def create_con(self, row, endpoint_id, print_log=True):
        self.driver = row['driver']
        self.hostname = row.get('hostname', None)
        ipaddress = row.get('ipaddress', None)
        self.port = int(row['port'])
        self.default_db = row.get('database', 'default')
        jdbc_option = row.get('jdbc_option', None)
        self.username = row['username']
        self.password = row['password']
        self.database_type = row['database_type']

        if self.hostname is None:
            if ipaddress is None:
                raise Exception('Both hostname and ipaddress is empty')
            self.hostname = ipaddress
        
        if print_log is True:
            print(f'\tConnecting to database {self.endpoint_type} {self.driver} {self.username} @ {self.hostname}:{self.port}')
        
        jdbc_url = f"{self.driver}://{self.username}:{quote(self.password)}@{self.hostname}:{self.port}/{self.default_db}"
                
        if jdbc_option is not None:
            jdbc_url = f'{jdbc_url}?{jdbc_option}'
            
        self.engine = create_engine(jdbc_url)
        self.conn = self.engine.connect()
        self.raw_conn = self.engine.raw_connection()
        
        Session = sessionmaker()
        Session.configure(bind=self.engine)
        self.session = Session()
        
        if print_log is True:
            print(f'Create {self.endpoint_type} {self.driver} {endpoint_id} Connection Successfully\n\n')
        
        
        
    ##########################################
    # Execute Query
    ##########################################
    def clean_script(self, script):
        script = script.strip()
        if script[-1] != ';':
            script += ';'
        return script
        
        
    def execute(self, script):
        print('Querying : ')
        start_time = time.time()
        
        script = self.clean_script(script)
        with self.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            results = conn.execute(script)
            try:
                results = results.fetchall()
            except Exception:
                results = ''
        time.sleep(3)
        
        end_time = time.time()
        print(f'\tCount {len(results):,} rows')
        print(f'Used_time : {np.round(end_time-start_time, 2)} s')
        return results
    
    
    def execute_df(self, script):
        return self.execute_pandas(script)
        
    
    def execute_pandas(self, script):
        print('Querying data to Pandas : ')
        start_time = time.time()
        
        script = self.clean_script(script)
        df = None
        with self.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            df = pd.read_sql(script, conn)
        time.sleep(3)
        
        end_time = time.time()
        print(f'\tCount {len(df):,} rows')
        print(f'\tUsed_time : {np.round(end_time-start_time, 2)} s')
        return df
    
    
    
#     def call_procedure(self, script):
#         print('Calling procedure : ')
#         start_time = time.time()
        
#         cursor = self.raw_conn.cursor()
#         cursor.callproc(script)
#         results = list(cursor.fetchall())
#         cursor.close()
#         self.raw_conn.commit()
        
#         end_time = time.time()
#         print(f'\tUsed_time : {np.round(end_time-start_time, 2)} s')
#         return results
    
    
    
    ##########################################
    # Insert
    ##########################################
    def insert_df(self, df, table_name, schema=None, truncate=False):
        database_name = self.engine.url.database
        if schema is None:
            destination = f"{database_name}.{table_name}"
        else:
            destination = f"{database_name}.{schema}.{table_name}"
           
        if truncate is True:
            print(f'Truncate Table {destination} : ')
            self.truncate(f'{destination}')
        
        print(f'Inserting Table {destination} : ')
        start_time = time.time()
        df.to_sql(table_name, con=self.engine, schema=schema, if_exists='append', index=False)
        end_time = time.time()
        print(f'\tUsed_time : {np.round(end_time-start_time, 2)} s')
    
    
    ##########################################
    # Import
    ##########################################
    def command_import(self, table, input_path, delimeter='|', optional_arguments=None):
        pass
    
    def import_data(self, table, input_path, delimeter='|', optional_arguments=None):
        pass
    
    ##########################################
    # Export
    ##########################################
    def command_export(self, script, output_path, delimeter='|'):
        pass
        
    def export_data(self, script, output_path, delimeter='|'):
        pass

    ###########################################
    # Fetch
    ###########################################
    def generate_sql_fetch(self, script, row_start=None, row_end=None):
        pass

    ##########################################
    # Utility
    ##########################################
    def truncate(self, table):
        truncate_script = f'truncate table {table}'
        print(f'Truncating table : {table}')
        print(truncate_script)
        self.execute(truncate_script)
        print(f'Done truncate table : {table}\n\n')
        
        
#     def generate_count_row_sql(self, sql):
#         temp_sql = sql.upper()
#         start = temp_sql.find('SELECT') + len("SELECT")
#         end = temp_sql.find(' FROM ') + 1
#         count_sql = sql[:start] + ' count(*) ' + sql[end:]
        
#         order_by = count_sql.upper().find('ORDER BY')
#         if order_by != -1:
#             count_sql = count_sql[:order_by].strip(' ;') + ';'

#         return count_sql
    
    def generate_count_row_sql(self, table, condition):
        return f"select count(*) from {table} {condition}"