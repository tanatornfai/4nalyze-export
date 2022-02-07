import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

import time
import boto3
import pandas as pd
import numpy as np
from helper.file import file_manager
from helper.utility.param_helper import read_parameter

class rs_helper:
    endpoint_type = 'RS'
    rs_client = None
    account = None
    username = None
    cluster_name = None
    database = None
    
    def __init__(self, endpoint_id, authentication = True):
        self.create_con(endpoint_id, authentication)
    
    
    
    ####################################################################
    ## CONNECTION
    ####################################################################
    def authen_account(self, access_key, secret_key, token = None):
        if token is None:
            self.rs_client = boto3.client(
                'redshift-data',
                aws_access_key_id = access_key,
                aws_secret_access_key = secret_key,
                region_name= self.region,
                use_ssl=True, 
                verify=True
            )
            
        else:
            self.rs_client = boto3.client(
                'redshift-data', 
                aws_access_key_id = access_key,
                aws_secret_access_key = secret_key,
                aws_session_token = token,
                region_name = self.region,
                use_ssl = True, 
                verify = True
            )
    
    
    def assume_role(self, access_key, secret_key, account, role):
        sts_client = boto3.client(
            'sts',
            aws_access_key_id = access_key,
            aws_secret_access_key = secret_key
        )
        
        assumed_role_object = sts_client.assume_role(
            RoleArn=f"arn:aws:iam::{account}:role/{role}",
            RoleSessionName = "AssumeRoleSession1",
            DurationSeconds = 43200 ,
        )
        credentials = assumed_role_object['Credentials']
        access_key = credentials['AccessKeyId']
        secret_key = credentials['SecretAccessKey']
        token = credentials['SessionToken']
        
        self.authen_account(access_key, secret_key, token)
    
    
    def create_con(self, endpoint_id, authentication):
        print(f'Creating {self.endpoint_type.upper()} {endpoint_id} Endpoint')
        
        if authentication:
            print('\tGetting Credential')
            row = file_manager.get_credential(self.endpoint_type, endpoint_id, validate_endpoint = True)
            self.account = row['account']
            access_key = row.get('access_key', None)
            secret_key = row.get('secret_key', None)
            role = row.get('role', None)
            self.region = row['region']
            self.database = row['database']
            self.cluster_name = row['cluster_name']
            self.username = row['username']

            if access_key is not None and secret_key is not None:
                if role is None:
                    print('\tAuthenticating using Accesskey and Secretkey')
                    self.authen_account(access_key, secret_key)
                    print(f'Create {self.endpoint_type} {endpoint_id} Connection Successfully\n\n')
                else:
                    print('\tAssuming Role')
                    self.assume_role(access_key, secret_key, account, role)
                    print(f'Create {self.endpoint_type} {endpoint_id} Connection Successfully\n\n')
            else:
                print('\tAccessKey or SecretKey not found, switch to No authentication mode (AWS ROLE)')
                self.create_con(endpoint_id, authentication = False)
                
        else:
            print('\tUsing AWS Role')
            self.rs_client = boto3.client(
                'redshift-data',
                region_name = self.region,
                use_ssl = True, 
                verify = True
            )
            print(f'Create {self.endpoint_type} {endpoint_id} Connection Successfully\n\n')
            

            
    ##########################################
    # Execute Query
    ##########################################        
    def clean_script(self, script):
        script = script.strip()
        if script[-1] != ';':
            script += ';'
        return script
    
    
    def execute(self, script,database): #execute create table
        print('Executing : ')
        print(script)
        start_time = time.time()
        query_id = self.rs_client.execute_statement(
                                                       Sql = script,
                                                       ClusterIdentifier=self.cluster_name,
                                                       Database= database,
                                                       DbUser=self.username
                                                                    )['Id']
        status_df = self.track_sql(id_list=[query_id],poke_interval=5)
        end_time = time.time()
        print(f'\tUsed_time : {np.round(end_time-start_time, 2)} s')
        return status_df
    
    
    def execute_df(self, script,database): #
        return self.execute_pandas(script,database)
        
    
    def execute_pandas(self,script,database): #Query and get result as Pandas
        print('Querying data to Pandas : ')
        start_time = time.time()
        query_id = self.rs_client.execute_statement(
                                                     Sql = script,
                                                     ClusterIdentifier=self.cluster_name,
                                                     Database= database,
                                                     DbUser=self.username
                                                                    )['Id']
        status_df = self.track_sql(id_list=[query_id],poke_interval=5)
        raw_result = self.rs_client.get_statement_result(Id=query_id)
        df = self.result_to_df(raw_result)
        end_time = time.time()
        print(f'\tCount {len(df):,} rows')
        print(f'\tUsed_time : {np.round(end_time-start_time, 2)} s')
        return df
    
    ##########################################
    # Insert
    ##########################################    
    def insert_df(self, df, database, table):
        columns = list(df.columns)
        for index, row in df.iterrows():
            values = [str(row[key]).replace("'", "''") for key in columns]
            values = [f"'{value}'" for value in values]
            sql_script = self.generate_insert_sql(database, table, columns, values)
            self.execute(sql_script, database)
            
    
    
    ##########################################
    # Import
    ##########################################
    def truncate(self,table,database):
        truncate_script = f'truncate table {table}'
        print(f'Truncating table : {table}')
        self.execute(truncate_script,database)
        print(f'Done truncate table : {table}')
    def load_data(self, table, data_path, iamrole,database, additional_arg = ''):
        load_script = self.copy_sql(table, data_path, iamrole,additional_arg = additional_arg)
        
        print(f'Importing data from {data_path} to table {table}')
        status_df = self.execute(load_script,database)
        return status_df
    
    ##########################################
    # Export
    ##########################################
    def export_data(self, script, data_path, iamrole,database, additional_arg = ''):
        export_script = self.unload_sql(script, data_path, iamrole,additional_arg = additional_arg)
       
        print(f'Exporting data using {script} to {data_path}')
        status_df = self.execute(export_script,database)
        return status_df
    
    
    ##########################################
    # SQL Generator
    ##########################################
    def copy_sql(self, table, filepath, iamrole,additional_arg = ''):
        sql = f"""
        copy {table}
        from '{filepath}'
        iam_role 'arn:aws:iam::{self.account}:role/{iamrole}'
        region '{self.region}'
        {additional_arg};
        """
        return sql
    
    def unload_sql(self, script, filepath, iamrole,additional_arg = ''):
        sql = f"""
            unload ('{script}') 
            to '{filepath}' 
            iam_role 'arn:aws:iam::{self.account}:role/{iamrole}'
            {additional_arg}; 
        """
        return sql
    
     ##########################################
    # Utility
    ##########################################  
    def check_status_exists(self, df, status_list):
        return (df['status'].isin(status_list)).any()
    
    def count_status(self, df, status):
        return (df['status'] == status).sum()
    
    def get_status_detail(self, job_id):
        '''
        Get Error correspond to query_id
        '''
        response = self.rs_client.describe_statement(Id = job_id)
        
        if response['Status'] == 'FINISHED':
            return response['Status'], ''
        elif response['Status'] == 'FAILED':
            return response['Status'], response['Error']
        else:
            return response['Status'], 'Undefined Status'
    def count_active_status(self,status):
        res = self.rs_client.list_statements(MaxResults=100,Status=status)
        no_statement = len(res['Statements'])
        while 'NextToken' in res.keys():
            res = self.rs_client.list_statements(MaxResults=100,Status=status,NextToken = res['NextToken'])
            no_statement = no_statement + len(res['Statements'])
        return no_statement
    def get_active_sql(self):
        submit_active = self.count_active_status('SUBMITTED')
        pick_active = self.count_active_status('PICKED')
        start_active = self.count_active_status('STARTED')
        active = submit_active+pick_active+start_active
        return active
    
    def track_sql(self, id_list, poke_interval=5):
        df = pd.DataFrame(
            columns = ['Id'],
            data = id_list
        ).set_index('Id')
        df['status'] = 'SUBMITTED'
        df['detail'] = None
        start_time = time.time()
        i = 0
        not_finish_status = ['SUBMITTED', 'PICKED', 'STARTED', 'RUNNING']
        while self.check_status_exists(df, not_finish_status):
            
            running_df = df[df['status'].isin(not_finish_status)]
            for index, row in running_df.iterrows():
                row['status'], row['detail'] = self.get_status_detail(index)
                df.loc[index] = row
                
            #Print Status (Can Comment Out)
            sub_count = self.count_status(df, 'SUBMITTED')
            picked_count = self.count_status(df, 'PICKED')
            start_count = self.count_status(df, 'STARTED')
            run_count = self.count_status(df, 'RUNNING')
            fin_count = self.count_status(df, 'FINISHED')
            err_count = self.count_status(df, 'FAILED')
            
            i = i + 1
            print(f'\t\tRound {i} : {time.time()-start_time} : SUBMITTED {sub_count} | PICKED {picked_count} | STARTED {start_count} | RUNNING {run_count} | FINISHED {fin_count} | ERROR {err_count}')
            if self.check_status_exists(df, not_finish_status):
                time.sleep(poke_interval)
        if self.count_status(df, 'FAILED') != 0:
            print(df.head())
            raise Exception("Executing ERROR")
        return df
    
    def result_to_df(self,response):
        columns = [key['name'] for key in response['ColumnMetadata']]
        data_list = []

        for row in  response['Records']:
            data = []
            for col in row:
                if 'isNull' in col.keys():
                    value = None
                else:
                    value = list(col.values())[0]
                data.append(value)
            data_list.append(data)
        df = pd.DataFrame(data_list, columns=columns)
        return df
        
    def get_schema(self, database, table):
        tmp_df = self.execute_df(f'select * from {table}', database)
        schema = list(tmp_df.columns)
        return schema
    
    def reorder_column(self, database, table, df):
        column_list = self.get_schema(database, table)
        print(f' - Table : {database}.{table}')
        print(f' - Column_list : {column_list}')
        df = df[column_list]
        return df
    
    def generate_insert_sql(self, database, table, columns, values):
        columns = ','.join(columns)
        values = ','.join(values)
        sql_script = f'insert into {database}.{table} ({columns}) values ({values})'
        return sql_script