import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

import boto3
import fnmatch
import pandas as pd

from io import BytesIO 

from helper.file import file_manager



class s3_helper:
    endpoint_type = 'S3'
    s3_client = None
    s3_resource = None
    s3_bucket = None
    bucket_name = None
    
    def __init__(self, endpoint_id, authentication = True):
        self.create_con(endpoint_id, authentication)
        self.bucket_name = endpoint_id
        self.s3_bucket = self.s3_resource.Bucket(endpoint_id)
    
    
    
    ####################################################################
    ## CONNECTION
    ####################################################################
    def authen_account(self, access_key, secret_key, token = None):
        if token is None:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id = access_key,
                aws_secret_access_key = secret_key
            )

            self.s3_resource = boto3.resource(
                's3',
                aws_access_key_id = access_key,
                aws_secret_access_key = secret_key
            )
        else:
            self.s3_client = boto3.client(
                's3', 
                aws_access_key_id = access_key,
                aws_secret_access_key = secret_key,
                aws_session_token = token,
            )

            self.s3_resource = boto3.resource(
                's3',
                aws_access_key_id = access_key,
                aws_secret_access_key = secret_key,
                aws_session_token = token,
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
        
        self.authen_account(access_key, secret_key)
    
    
    def create_con(self, endpoint_id, authentication):
        print(f'Creating {self.endpoint_type.upper()} {endpoint_id} Endpoint')
        
        if authentication:
            print('\tGetting Credential')
            row = file_manager.get_credential(self.endpoint_type, endpoint_id, validate_endpoint = False)
            if len(row) > 0:        
                account = row.get('account', None)
                access_key = row.get('access_key', None)
                secret_key = row.get('secret_key', None)
                role = row.get('role', None)

                if access_key is not None and secret_key is not None:
                    if account is None or role is None:
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
                print('\tCredential not found, switch to No authentication mode (AWS ROLE)')
                self.create_con(endpoint_id, authentication = False)
        else:
            print('\tUsing AWS Role')
            self.s3_client = boto3.client('s3')
            self.s3_resource = boto3.resource('s3')
            print(f'Create {self.endpoint_type} {endpoint_id} Connection Successfully\n\n')
        
            
            
    ####################################################################
    ## READ
    ####################################################################
    def read_csv(self, file_path, config_dict = dict()):
        print(f'Reading csv file : {file_path}')
        if len(config_dict) > 0:
            print('read option : ')
            print(config_dict)
        delimiter = config_dict.get('delimiter', None)
        header_row = config_dict.get('header_row', 'infer')
        nrows = config_dict.get('nrows', None)
        
        body = self.get_content(file_path)
        data = BytesIO(body)
        
        df = pd.read_csv(
            data,
            delimiter = delimiter,
            header = header_row,
            nrows = nrows
        )
        df = df.where(pd.notnull(df), None)
        return df
        
        
    def read_excel(self, file_path, config_dict = dict()):
        pass
        
        
    def read_json(self, file_path, config_dict = dict()):
        pass
        
        
    def read_parquet(self, file_path, config_dict = dict()):
        pass
    
    
    def read_text(self, file_path, config_dict = dict()):
        if len(config_dict) > 0:
            print('read option : ')
            print(config_dict)
        encoding = config_dict.get('encoding', 'utf-8')
        
        body = self.get_content(file_path)
        text = body.decode(encoding)
        return text
    
    
    def read_lines(self, file_path, config_dict = dict()):
        text = self.read_text(file_path, config_dict)
        nrows = config_dict.get('nrows', None)
        
        lines = text.split('\n')
        lines = [line.rstrip() for line in lines]
        if nrow is not None:
            lines = lines[:nrows]
        return lines
    
    
    def read_file(self, file_path, config_dict = dict()):
        if file_path.endswith('.csv'):
            content = self.read_csv(file_path, config_dict)
            
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
    def get_content(self, file_path):
        obj = self.get_file(file_path)
        body = obj.get()['Body'].read()
        return body
    
    
    def get_file(self, file_path):
        """
        Get file that that match file_path and file_name 
        Only 1 matched file is allowed
        """
        
        obj_list = self.list_file(file_path, return_obj = True)
        
        if len(obj_list) == 0:
            raise Exception(f'File not found in path : "{file_path}"')    
            
        elif len(obj_list) > 1:
            example_files = "\n".join([obj.key for obj in obj_list])
            raise Exception(f'Multiple files found in path : "{file_path}" \n{example_files}')
            
        else:
            return obj_list[0]
    
    
    def list_file(self, file_path, show_log = True, return_obj = False, include = 'file'):
        print(f'Listing file : ')
        print(f' - File_path : {file_path}')
        obj_list = self.list_obj(file_path, show_log)
        
        if include == 'file':
            obj_list = [obj for obj in obj_list if obj.key[-1] != '/']
        elif include == 'dir':
            obj_list = [obj for obj in obj_list if obj.key[-1] == '/']
        
        if return_obj:
            return obj_list
        else:
            file_list = [obj.key for obj in obj_list]
            return file_list
 

    def list_obj(self, file_path, show_log = True):
        file_path_list = file_path.split('*',1)
        if len(file_path_list) == 2:
            file_path, file_name = file_path_list
        else:
            file_name = ''

        obj_list = [obj for obj in self.s3_bucket.objects.filter(Prefix = file_path)]
        
        if file_name != '':
            obj_list = [obj for obj in obj_list if fnmatch.fnmatchcase(obj.key, f'*{file_name}')]
        
        if show_log:
            print(f' - File_name : {file_name}')
            print(f' - Count : {len(obj_list)} files')
            for obj in obj_list:
                print(f'\t{obj.key}')
        
        return obj_list

 
    
    ####################################################################
    ## MANAGE - DELETE, MOVE, COPY
    ####################################################################
    def delete(self, file_path):
        delete = {
            'Objects': [{'Key': file_path}],
            'Quiet': False
        }
        self.s3_bucket.delete_objects(Delete=delete)
    
    
    def delete_file(self, file_path, show_log = True):
        print(f'Deleting {self.endpoint_type} path :')
        print(f' - Path : {file_path}')
        
        file_list = self.list_file(file_path, show_log)
        
        for file_path in file_list:
            _, file_name, folder_path = file_manager.get_file_infor(file_path)
            print(f'\tDeleting {file_path}')
            self.delete(file_path)
    
    
    def move_file(self, source_path, destination_path, clear_destination = True, show_log = True):
        self.copy_file(source_path, destination_path, clear_destination, show_log)
        self.delete_file(source_path, show_log = True)
    
    
    def copy(self, source_file_path, destination_file_path):
        copy_source = {
            'Bucket': self.bucket_name,
            'Key': source_file_path
        }
        self.s3_bucket.copy(copy_source, destination_file_path)
    
    
    def copy_file(self, source_path, destination_path, clear_destination = True, show_log = True):
        if clear_destination:
            print(f'Clearing destination : ')
            self.delete_file(destination_path, show_log = True)
            print()
        
        # Check Source Path
        print(f'Source path : ')
        source_file_list = self.list_file(source_path, show_log)
        print()
        
        # Copy
        print(f'Copying {self.endpoint_type} File : ')
        print(f' - From {source_path}')
        print(f' - To {destination_path}')
        for source_file_path in source_file_list:
            _, file_name, folder_path = file_manager.get_file_infor(source_file_path)
            folder_path = folder_path.replace(source_path, destination_path, 1)
            destination_file_path = source_file_path.replace(source_path, destination_path, 1)
            
            print(f'\t{file_name}')
            print(f'\t\tfrom {source_file_path}')
            print(f'\t\tto {destination_file_path}')
            self.copy(source_file_path, destination_file_path)
        print()
        
        # Check Source Path
        print(f'Destination path : ')
        file_list = self.list_file(destination_path, show_log)
        print()
        

        
    ####################################################################
    ## DOWNLOAD & UPLOAD
    ####################################################################
    def download(self, source_file_path, destination_file_path):
        self.s3_bucket.download_file(source_file_path, destination_file_path)
        
    
    def download_file(self, source_path, destination_path, clear_destination = True, show_log = True):
        if clear_destination:
            print(f'Clearing Destination (local):')
            file_manager.delete_file(destination_path)
            print()
            
        # Check Source Path
        print(f'Source path ({self.endpoint_type}) : ')
        source_file_list = self.list_file(source_path, show_log)
        print()
        
        # Download
        print(f'Downloading : ')
        print(f' - From {source_path}')
        print(f' - To {destination_path}')
        for source_file_path in source_file_list:
            _, file_name, folder_path = file_manager.get_file_infor(source_file_path)
            destination_file_path = source_file_path.replace(source_path, destination_path, 1)
            
            file_manager.ensure_dir(destination_file_path)
                
            print(f'\t{file_name}')
            print(f'\t - from {source_file_path}')
            print(f'\t - to {destination_file_path}')
            self.download(source_file_path, destination_file_path)
        print()
            
        # Check Destination Path
        print(f'Destination path (local): ')
        file_list = file_manager.list_file(destination_path, show_log)
        print()
    
    
    def upload(self, source_file_path, destination_file_path):
        self.s3_client.upload_file(
            source_file_path, 
            self.bucket_name, 
            destination_file_path
        )
    
    
    def upload_file(self, source_path, destination_path, clear_destination = True, show_log = True):
        if clear_destination:
            print(f'Clearing Destination ({self.endpoint_type}):')
            self.delete_file(destination_path, show_log = True)
            print()
        
        # Check Source Path
        print(f'Source path (local) : ')
        source_file_list = file_manager.list_file(source_path, show_log)
        print()
        
        # Upload
        print(f'Uploading : ')
        print(f' - From {source_path}')
        print(f' - To {destination_path}')
        for source_file_path in source_file_list:
            _, file_name, folder_path = file_manager.get_file_infor(source_file_path)
            destination_file_path = source_file_path.replace(source_path, destination_path, 1)
            
            self.ensure_dir(destination_file_path)
                
            print(f'\t{file_name}')
            print(f'\t - from {source_file_path}')
            print(f'\t - to {destination_file_path}')
            self.upload(source_file_path, destination_file_path)
        print()
    
        # Check Destination Path
        print(f'Destination path ({self.endpoint_type}) : ')
        file_list = self.list_file(destination_path, show_log)
        print()
    
    
    ####################################################################
    ## UTILITY
    ####################################################################
    def get_file_size(self, file_path):
        obj = self.s3_resource.Object(self.bucket_name, file_path)
        file_size = obj.content_length
        return file_size
        
        
    def check_file_exists(self, file_path):
        file_list = list_file(self, file_path, show_log = False, return_obj = False)
        file_list = [x for x in file_list if x == file_path]
        if len(file_path) == 1:
            return True
        else:
            return False
        
        
    def ensure_dir(self, file_path, show_log = True):
        # s3 don't have directory
        pass
        
        
    def create_dir(self, dir_path, show_log = True):
        # s3 don't have directory
        pass