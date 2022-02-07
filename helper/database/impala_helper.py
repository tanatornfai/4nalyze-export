import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

import time
import ibis
# import fastparquet

import numpy as np
import pandas as pd

from helper.file import file_manager
from helper.file.hdfs_helper import hdfs_helper
from helper.utility.time_helper import *

# from impala.dbapi import connect
# from impala.util import as_pandas

class impala_helper():
    endpoint_type = 'IMPALA'
    hostname = None
    username = None
    impala_client = None
    hdfs = None
    tmp_folder = None
    airflow_root = '/usr/local/airflow'
    
    def __init__(self, endpoint_id, hdfs, tmp_folder = '/tmp/ibis/'):
        self.hdfs = hdfs
        if tmp_folder[-1] != '/':
            tmp_folder += '/'
        self.tmp_folder = tmp_folder
        self.create_con(endpoint_id)
    
    
    
    ##########################################
    # Create Connection
    ##########################################
    def create_con(self, endpoint_id):
        print(f'Creating {self.endpoint_type.upper()} {endpoint_id} Endpoint')
        
        print('\tGetting Credential')
        row = file_manager.get_credential(self.endpoint_type, endpoint_id, validate_endpoint = True)
        self.hostname = row.get('hostname', None)
        ipaddress = row.get('ipaddress', None)
        port = int(row.get('port', None))
        auth_mechanism = row.get('auth_mechanism', None)
        use_ssl = row.get('use_ssl', None)
        default_db = row.get('database', 'default')
        self.username = row.get('username', None)
        password = row.get('password', None)

        if self.hostname is None:
            if ipaddress is None:
                raise Exception('Both hostname and ipaddress is empty')
            self.hostname = ipaddress

        print(f'\tConnecting to database {self.endpoint_type} {self.username} @ {self.hostname}:{port}')
#         print(self.hostname, port, auth_mechanism, use_ssl, self.username, password, default_db)
#         self.impala_client = ibis.impala.connect(
#             host = self.hostname, 
#             port = port, 
#             hdfs_client = self.hdfs.hdfs_client, 
#             auth_mechanism = auth_mechanism, 
#             use_ssl = use_ssl, 
#             user = self.username, 
#             password = password, 
#             database = default_db
#         )
        
        self.impala_client = connect(host='10.251.8.24', port=21050, user='infa', password='P@ssw0rd', auth_mechanism='LDAP', database='default')
        print(f'Create {self.endpoint_type} {endpoint_id} Connection Successfully\n\n')
        
        
        
    ##########################################
    # Execute Query
    ##########################################
    def clean_script(self, script):
        script = script.strip()
        if script[-1] != ';':
            script += ';'
        return script
    
    def execute_raw_sql(self, script):
        return self.execute(script)
    
    def execute(self, script):
        print('Querying : ')
        start_time = time.time()
        
        cursor = self.impala_client.cursor()
        cursor.execute(script)
        try:
            results = cursor.fetchall()
        except Exception:
            results = ''
        
        end_time = time.time()
        print(f'\tCount {len(results):,} rows')
        print(f'Used_time : {np.round(end_time-start_time, 2)} s')
        return results
    
    def refresh_table(self, database, table):
        print(f'REFRESHING TABLE {database}.{table}')
        self.execute(f'REFRESH {database}.{table}')
    
    def execute_and_refresh(self, script):
        print('Script : ')
        print(script)
        
        self.execute(script)
        time.sleep(2)
        self.execute('INVALIDATE METADATA')
#         self.impala_client.invalidate_metadata()
        time.sleep(2)
    
    
    def execute_list(self, script):
        results = self.execute(script)
        return results
    
    
    def execute_df(self, script):
        return self.execute_pandas(script)
        
    
    def execute_pandas(self, script):
        print('Querying to Pandas : ')
        start_time = time.time()
        
#         request = self.impala_client.sql(script)
#         df = request.execute(limit = None)

        
        cursor = self.impala_client.cursor()
        cursor.execute(script)
        try:
            df = as_pandas(cursor)
        except Exception:
            df = pd.DataFrame()
        
        end_time = time.time()
        print(f'\tCount {len(df):,} rows')
        print(f'Used_time : {np.round(end_time-start_time, 2)} s')
        return df
    
    
    ##########################################
    # Insert
    ##########################################
    def gen_script_add_partition(self, database, table, partition_list = None):
        # Alter Partition if not exists
        if partition_list is not None and isinstance(partition_list, list):
            alter_sql = f"ALTER TABLE {database}.{table} ADD IF NOT EXISTS"
            alter_sql += f" PARTITION ("
            for partition_column, partition_value in partition_list:
                if not str(partition_value).isnumeric():
                    partition_value = f'"{str(partition_value)}"'
                alter_sql += f"{partition_column} = {partition_value},"
            alter_sql = alter_sql[:-1]
            alter_sql += ')'
        return alter_sql
    
    def add_partition(self, database, table, partition_list):
        if partition_list is not None and isinstance(partition_list, list):
            print('Add Partition')
            for partition_column, partition_value in partition_list:
                print('\t', partition_column, partition_value)
            alter_script = self.gen_script_add_partition(database, table, partition_list)
            
            self.execute_and_refresh(alter_script)
    
    def gen_script_load_data(self, database, table, hdfs_file_path, overwrite, partition_list = None):
        if not overwrite:
            overwrite = ''
        else:
            overwrite = 'OVERWRITE'
        
        sql = f"LOAD DATA INPATH '{hdfs_file_path}' {overwrite} INTO TABLE {database}.{table}"
        if partition_list is not None and isinstance(partition_list, list):
            sql += f" PARTITION ("
            for partition_column, partition_value in partition_list:
                if not str(partition_value).isnumeric():
                    partition_value = f'"{str(partition_value)}"'
                sql += f"{partition_column} = {partition_value},"
            sql = sql[:-1]
            sql += ')'
        return sql
    
    def load_data(self, database, table, hdfs_file_path, overwrite, partition_list):
        load_script = self.gen_script_load_data(database, table, hdfs_file_path, overwrite, partition_list)
        
        self.execute_and_refresh(load_script)
    
    def reorder_column(self, database, table, df):
        column_list = self.get_schema(database, table)
        print(f' - Table : {database}.{table}')
        print(f' - Column_list : {column_list}')
        df = df[column_list]
        return df
    
    def export_local(self, df, local_folder, file_name):
        file_manager.create_dir(local_folder)
        print(f'\tlocal        : {local_folder}')
        print(f'\tfile_name    : {file_name}' )
        print(f'\tcolumn count : {len(df.columns)} columns')
        print(f'\trow count    : {len(df):,} rows')
        fastparquet.write(local_folder + file_name, df, file_scheme='simple', write_index = False)
        
    def prepare_hdfs_data_dir(self, db_folder, tb_folder):
        print(f'\ttmp folder : {tb_folder}')
        self.hdfs.create_dir(self.tmp_folder)
        self.hdfs.create_dir(db_folder)
        self.hdfs.delete_dir(tb_folder)
        self.hdfs.create_dir(tb_folder)
    
    def get_local_path(self, database, table, local_tmp_dir, process_date, job_name):
        if local_tmp_dir is None:
            local_tmp_dir = f'{self.airflow_root}/tmp/{database}/{table}/{job_name}/'
        file_name = f'{database}_{table}_{process_date}.parquet'
        
        if local_tmp_dir[-1] != '/':
            local_tmp_dir += '/'
        local_file_path = local_tmp_dir + file_name
        return local_tmp_dir, file_name, local_file_path
        
    def get_hdfs_path(self, database, table, file_name, job_name):
        db_folder = f'{self.tmp_folder}{database}/'
        tb_folder = f'{db_folder}{table}/'
        job_folder = f'{tb_folder}{job_name}/'
        hdfs_file_path = job_folder + file_name
        return db_folder, tb_folder, job_folder, hdfs_file_path
    
    
    def export_parquet(self, df, database, table, local_tmp_dir = None, process_date = None, job_name = 'default'):
        start_time = time.time()
        
        database, table = self.get_info(database, table)
        local_tmp_dir, file_name, local_file_path = self.get_local_path(database, table, local_tmp_dir, process_date, job_name)
        
        print("\n[ Reordering column ]")
        df = self.reorder_column(database, table, df)
        
        
        print('\n[ Preparing Tmp (Local) ]')
        file_manager.delete_file(local_tmp_dir)
        file_manager.create_dir(local_tmp_dir)
        
        
        print('\n[ Exporting to Tmp (Local) ]')
        print(f' - Local Tmp Dir : {local_tmp_dir}')
        print(f' - File Name     : {file_name}')
        print(f' - Column count  : {len(df.columns)} columns')
        print(f' - Row count     : {len(df):,} rows')
        fastparquet.write(local_file_path, df, file_scheme = 'simple', write_index = False)
        print('\nSuccess')
        
        
        print('\n[ Checking Tmp (Local) ]')
        file_manager.list_file(file_path = local_tmp_dir)
        
        
        end_time = time.time()
        print(f'\tExport time : {np.round(end_time-start_time,2):,} s')
        
    def prepare_tmp_hdfs(self, local_tmp_dir, db_folder, tb_folder, job_folder, hdfs_file_path):
        print('\n[ Preparing Tmp (HDFS) ]')
        self.hdfs.delete_file(job_folder)
        self.hdfs.list_file(job_folder)
        
        
        print('\n[ Uploading Tmp Local to HDFS ]')
        file_manager.list_file(local_tmp_dir)
        self.hdfs.upload(local_tmp_dir, job_folder)      
        
        
        print('\n[ Checking Tmp (HDFS) ]')
        self.hdfs.list_file(job_folder)


        print('\n[ Setting HDFS Permission ]')
        print(f'Set permission on {hdfs_file_path}')
        self.hdfs.chmod(db_folder, permission = '777')
        self.hdfs.chmod(tb_folder, permission = '777')
        self.hdfs.chmod(job_folder, permission = '777')
        self.hdfs.chmod(hdfs_file_path, permission = '777')
        
    
    def create_tmp_table(self, tmp_database, tmp_table, database, table):
        self.drop_table(tmp_database, tmp_table)
        self.create_table_from_select(tmp_database, tmp_table, database, table)
        
        
    def upsert_to_kudu(self, database, table, tmp_database, tmp_table, upsert = True):
        if upsert:
            upsert_script = f"UPSERT INTO {database}.{table} SELECT * FROM  {tmp_database}.{tmp_table}"
        else:
            upsert_script = f"INSERT INTO {database}.{table} SELECT * FROM  {tmp_database}.{tmp_table}"
        self.execute_and_refresh(upsert_script)

        
    def insert_impala(self, df, database, table, 
                      local_tmp_dir = None, process_date = None,
                      overwrite = False, partition_list = None, job_name = 'default'):

        self.export_parquet(df, database, table, local_tmp_dir, process_date, job_name)
        local_tmp_dir = self.import_impala(database, table, 
                                           local_tmp_dir, process_date, 
                                           overwrite, partition_list, job_name)
        file_manager.delete_file(local_tmp_dir)
        time.sleep(5)
        self.refresh_table(database, table)
        time.sleep(5)

    def upsert_kudu(self, df, database, table,
                    local_tmp_dir = None, process_date = None, upsert = True, job_name = 'default'):
        self.export_parquet(df, database, table, local_tmp_dir, process_date, job_name)
        local_tmp_dir = self.import_kudu(database, table, 
                                         local_tmp_dir, process_date, upsert, job_name)
        file_manager.delete_file(local_tmp_dir)
        time.sleep(5)
        self.refresh_table(database, table)
        time.sleep(5)


    ##########################################
    # Import
    ##########################################
    def import_impala(self, database, table, 
                      local_tmp_dir = None, process_date = None, 
                      overwrite = False, partition_list = None, job_name = 'default'):
        print('Importing File to Impala Table')
        
        start_time = time.time()
        
        database, table = self.get_info(database, table)
        
        local_tmp_dir, file_name, local_file_path = self.get_local_path(database, table, local_tmp_dir, process_date, job_name)
        db_folder, tb_folder, job_folder, hdfs_file_path = self.get_hdfs_path(database, table, file_name, job_name)

        print(f'\tdatabase : {database}')
        print(f'\table : {table}')
        print(f'\tlocal_tmp_dir : {local_tmp_dir}')
        print(f'\toverwrite : {overwrite}')
        print(f'\tpartition_list : {partition_list}')
        
        self.prepare_tmp_hdfs(local_tmp_dir, db_folder, tb_folder, job_folder, hdfs_file_path)

        print('\n[ Add Partition to Impala Table ]')
        self.add_partition(database, table, partition_list)
        
        
        print('\n[ Loading Data to Impala Table From Tmp HDFS ]')
        self.load_data(database, table, job_folder, overwrite, partition_list)


        print('\n[ Clearing Tmp HDFS ]')
        self.hdfs.delete_file(job_folder)
        
        
        print('\nSuccess')
        
        end_time = time.time()
        print(f'\tImport time : {np.round(end_time-start_time,2):,} s')
        
        return local_tmp_dir
    
    def import_kudu(self, database, table, 
                    local_tmp_dir = None, process_date = None, upsert = True, job_name = 'default'):
        print('Importing File to Kudu Table')
        
        start_time = time.time()
        
        database, table = self.get_info(database, table)
        
        local_tmp_dir, file_name, local_file_path = self.get_local_path(database, table, local_tmp_dir, process_date, job_name)
        db_folder, tb_folder, job_folder, hdfs_file_path = self.get_hdfs_path(database, table, file_name, job_name)

        
        print(f'\tdatabase : {database}')
        print(f'\ttable : {table}')
        print(f'\tlocal_tmp_dir : {local_tmp_dir}')
        
        self.prepare_tmp_hdfs(local_tmp_dir, db_folder, tb_folder, job_folder, hdfs_file_path)

        #tmp impala
        tmp_database = 'default'
        tmp_table = 'tmp_upsert_' + table
        
        print('\n[ Creating Tmp Impala Table ]')
        self.create_tmp_table(tmp_database, tmp_table, database, table)
        
        
        print('\n[ Loading Data to Tmp Impala Table From HDFS ]')
        self.load_data(tmp_database, tmp_table, job_folder, overwrite = True, partition_list = None)


        print('\n[ Upserting Data to Kudu Table From Tmp Impala Table ]')
        self.upsert_to_kudu(database, table, tmp_database, tmp_table, upsert)
        
        
        print('\n[ Dropping Tmp Impala Table ]')
        self.drop_table(tmp_database, tmp_table)


        print('\n[ Clearing Tmp HDFS ]')
        self.hdfs.delete_file(job_folder)
        
        
        print('\nSuccess')
        
        end_time = time.time()
        print(f'\tImport time : {np.round(end_time-start_time,2):,} s')
        
        return local_tmp_dir
    
    ##########################################
    # Export
    ##########################################
    
    
    ##########################################
    # Utility
    ##########################################
    def drop_table(self, database, table):
        drop_script = f"DROP TABLE IF EXISTS {database}.{table}"
        self.execute_and_refresh(drop_script)
        
    def create_table_from_select(self, tmp_database, tmp_table, database, table):
        create_script = f"""CREATE TABLE {tmp_database}.{tmp_table}
        STORED AS PARQUET
        AS SELECT * FROM {database}.{table}
        """
        self.execute_and_refresh(create_script)
    
    def get_schema(self, database, table):
#         schema = self.impala_client.get_schema(table_name = table, database = database)
        tmp_df = self.execute_df(f'select * from {database}.{table} limit 0')
        schema = list(tmp_df.columns)
        return schema
    
    def get_info(self, database, table):
        if database is None:
            if '.' in table:
                database, table = table.split('.',2)
            else:
                raise Exception(f'Database not found, table {table}, database {database}')
        return database, table