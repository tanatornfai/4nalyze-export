import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

import re

import pandas as pd

from datetime import datetime
from helper.database.db_helper import select_db_helper
from helper.file.file_manager import get_credential
from helper.database.rs_helper import rs_helper

##########################################
# Log
##########################################
class log_helper():
    db_helper     = None
    schema        = None
    table         = None
    endpoint_type = None
    
    def __init__(self, endpoint_id):
        row = get_credential(endpoint_type='log', endpoint_id=endpoint_id, validate_endpoint=False)
        db_endpoint_id = row['db_endpoint_id']
        self.endpoint_type = row['endpoint_type']
        self.schema = row.get('schema', None)
        self.table = row['table']
        if self.endpoint_type.lower() == 'db':
            self.db_helper = select_db_helper(db_endpoint_id)
        elif self.endpoint_type.lower() == 'rs':
            self.db_helper = rs_helper(db_endpoint_id)
        else:
            raise Exception('Endpoint Type Error!!!')
        print(f"Logging into Endpoint_Type {self.endpoint_type}, DB_Endpoint_ID {db_endpoint_id}, Schema {self.schema}, Table {self.table}")

    def gen_log_df(self, parameter, start_task_time, end_task_time, status, detail):
        log_list = [{
            'sub_zone_name': str(parameter['SUB_ZONE_NAME']),
            'job_name': str(parameter['JOB_NAME']),
            'job_type': str(parameter['JOB_TYPE']),
            'job_user': str(parameter['UPDATE_BY']),
            'job_startdate': str(start_task_time),
            'job_enddate': str(end_task_time),
            'job_status': str(status),
            'detail': str(detail),
            'etl_date': str(datetime.now()),
            'process_date': str(parameter['PROCESS_DATE']),
            'project_name': str(parameter['PROJECT_NAME']),
            'dag_name': str(parameter['DAG_NAME']),
            'zone_name': str(parameter['ZONE_NAME']),
        }]
        log_df = pd.DataFrame(log_list)
        log_df['job_startdate'] = pd.to_datetime(log_df['job_startdate'])
        log_df['job_enddate'] = pd.to_datetime(log_df['job_enddate'])
        log_df['etl_date'] = pd.to_datetime(log_df['etl_date'])
        return log_df

    def insert_log(self, parameter, start_task_time, end_task_time, status, detail):
        log_df = self.gen_log_df(parameter, start_task_time, end_task_time, status, detail)
        
        if self.endpoint_type.lower() == 'db':
            self.db_helper.insert_df(log_df, self.table, self.schema)
        elif self.endpoint_type.lower() == 'rs':
            database = self.db_helper.database
            table = f'{self.schema}.{self.table}'
            self.db_helper.insert_df(log_df, database, table)
        
        return log_df

#     def gen_log_script(self, log_df, database, table, partition_columns = None):
#         log_list = log_df.values[0]
#         log_list = [f"'{log}'" for log in log_list]
#         log_value = ','.join(log_list)
#         log_value = f'({log_value})'

#         if partition_columns is None:
#             partition_script = ''
#         else:
#             partition_script = ', '.join(partition_columns)
#             partition_script = f'PARTITION ({partition_script})'

#         log_script = f'''
#         INSERT INTO {database}.{table} {partition_script}
#         values {log_value}
#         '''
#         return log_script

#     def insert_log(self, parameter, start_task_time, end_task_time, status, detail,
#                    database, table, partition_columns = None):
#         log_df = self.gen_log_df(parameter, start_task_time, end_task_time, status, detail)
#         log_script = self.gen_log_script(log_df, database, table, partition_columns)

#         print(log_script)
#         self.db_helper.execute(log_script)

#         return log_df


#     def create_table(self, database, table):
#         # คิดว่าควรมี log table ไว้ใน database อยู่แล้ว เพราะว่าแต่ล่ะ databse 
#         # script create table อาจจะไม่เหมือนกัน
#         f'''
#         CREATE TABLE IF NOT EXISTS {database}.{table}(
#             sub_zone_name STRING,
#             job_name STRING,
#             job_type STRING,
#             job_user STRING,
#             job_startdate TIMESTAMP,
#             job_enddate TIMESTAMP,
#             job_status STRING,
#             detail STRING,

#             etl_date TIMESTAMP,
#             process_date STRING
#         )
#         PARTITIONED BY (
#             project_name STRING,
#             dag_name STRING,
#             zone_name STRING
#         )
#         STORED AS PARQUET;
#         '''