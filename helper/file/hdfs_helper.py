import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

import boto3
import ibis

from helper.file import file_manager
from helper.file import kerberos_helper



class hdfs_helper:
    endpoint_type = 'HDFS'
    hostname = None
    username = None
    hdfs_client = None
    
    def __init__(self, endpoint_id):
        self.create_con(endpoint_id)
    
    
    
    ####################################################################
    ## CONNECTION
    ####################################################################
    def create_con(self, endpoint_id):
        print(f'Creating {self.endpoint_type.upper()} {endpoint_id} Endpoint')
        
        print('\tGetting Credential')
        row = file_manager.get_credential(self.endpoint_type, endpoint_id, validate_endpoint = True)
        self.hostname = row.get('namenode_hostname', None)
        ipaddress = row.get('namenode_ipaddress', None)
        webhdfs_port = int(row.get('webhdfs_port', 9870))
        auth_mechanism = row['auth_mechanism']
        use_https = row.get('use_https', False)
        self.username = row['username']
        password = row['password']

        print('\tAuthenticating Kerberos using user and password via Kerberos')
        kerberos_helper.krbauth(self.username, password)
        
        if self.hostname is None:
            if ipaddress is None:
                raise Exception('Both hostname and ipaddress is empty')
            self.hostname = ipaddress
            
        print(f'\tConnecting to webhdfs @ {self.hostname}:{webhdfs_port} via {auth_mechanism} (HTTPS : {use_https})')
#         print(f'hostname : {self.hostname}')
#         print(f'webhdfs_port : {webhdfs_port}')
#         print(f'auth_mechanism : {auth_mechanism}')
#         print(f'use_https : {use_https}')
#         print(f'username : {self.username}')
#         print(f'password : {password}')
        
        
        self.hdfs_client = ibis.impala.hdfs_connect(
            host = self.hostname, 
            port = webhdfs_port, 
            auth_mechanism = auth_mechanism,
            use_https = use_https
        )
        
        print(f'Create {self.endpoint_type} {endpoint_id} Connection Successfully\n\n')
            
            
    ####################################################################
    ## READ
    ####################################################################
    def read_csv(self, file_path, config_dict = dict()):
        
        print('Not Implement yet :P')
        pass
        
    def read_excel(self, file_path, config_dict = dict()):
        
        print('Not Implement yet :P')
        pass
        
    def read_json(self, file_path):
        
        print('Not Implement yet :P')
        pass
        
    def read_parquet(self, file_path):
        
        print('Not Implement yet :P')
        pass
    
    def read_text(self, file_path):
        
        print('Not Implement yet :P')
        pass
    
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
    def list_file(self, file_path, show_log = True, return_obj = False, include = 'file'):
        if show_log:
            print(f'Listing file : ')
            print(f' - File_path : {file_path}')

        if not self.check_file_exists(file_path):
            return []

        if not self.is_dir(file_path):
            parent_file_path = file_path.rstrip('/').rsplit('/',1)[0]
            obj_list = self.list_obj(parent_file_path, show_log)
            obj_list = [(obj,filename) for obj,filename in obj_list if filename == file_path]
        else:
            obj_list = self.list_obj(file_path, show_log)


        if include == 'file':
            obj_list = [(file_path, infor_dict) for file_path,infor_dict in obj_list if infor_dict['type'] != 'DIRECTORY']
        elif include == 'dir':
            obj_list = [(file_path, infor_dict) for file_path,infor_dict in obj_list if infor_dict['type'] == 'DIRECTORY']


        if show_log:
            print(f' - Count : {len(obj_list)} files')
            for file_path, infor_dict in obj_list:
                print(f'\t{file_path}')

        if return_obj:
            return obj_list
        else:
            file_list = [file_path for file_path, infor_dict in obj_list]
            return file_list


    def list_obj(self, file_path, show_log = True, return_obj = False):
        if len(file_path) > 0 and file_path[-1] == '/':
            file_path = file_path[:-1]

        obj_list = []
        this_obj_list = self.hdfs_client.ls(file_path, status=True)

        for file_name, infor_dict in this_obj_list:
            this_file_path = f'{file_path}/{file_name}'
            is_dir = infor_dict['type'] == 'DIRECTORY'
            if is_dir:
                sub_obj_list = self.list_obj(this_file_path, show_log, return_obj)
                obj_list = obj_list + sub_obj_list
                obj_list.append([f'{this_file_path}/', infor_dict])
            else:
                obj_list.append([this_file_path, infor_dict])

        return obj_list
    
    
    
    ####################################################################
    ## MANAGE - DELETE, MOVE, COPY
    ####################################################################
    def delete_dir(self, dir_path):
        self.hdfs_client.rmdir(dir_path)
        pass
    
    
    def delete(self, file_path):
        pass
    
    
    def delete_file(self, file_path, show_log = True):
        print(f'Deleting {self.endpoint_type} path :')
        print(f' - Path : {file_path}')
        
        file_list = self.list_file(file_path, show_log = False)
        folder_list = self.list_file(file_path, show_log = False, include = 'dir')
        
        # delete file
        for file_path in file_list:
            print(f'\tDeleting {file_path}')
            self.delete(file_path)
            
        # delete folder
        for file_path in folder_list:
            print(f'\t{file_path}')
            self.delete_dir(file_path)
    
    
    def move_file(self, source_path, destination_path, clear_destination = True, show_log = True):
        self.copy_file(source_path, destination_path, clear_destination, show_log)
        self.delete_file(source_path, show_log = True)
    
    
    def copy(self, source_file_path, destination_file_path):
        pass
    
    
    def copy_file(self, source_path, destination_path, clear_destination = True, show_log = True):
        pass
        

        
    ####################################################################
    ## DOWNLOAD & UPLOAD
    ####################################################################
    def download(self, source_file_path, destination_file_path):
        pass
        
    
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
        self.hdfs_client.put(
            hdfs_path = destination_file_path, 
            resource = source_file_path, 
            overwrite = True, 
            verbose = True
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
        pass
        
        
    def check_file_exists(self, file_path):
        try:
            self.hdfs_client.status(file_path)
            return True
        except Exception as e:
            return False

        
    def is_dir(self, file_path):
        is_dir = self.hdfs_client.status(file_path)['type'] == 'DIRECTORY'
        return is_dir
        
        
    def ensure_dir(self, file_path, show_log = True):
        pass
        
        
    def create_dir(self, dir_path, show_log = True):
        self.hdfs_client.mkdir(dir_path)
    
    
    def chown(self, file_path, user, group = None, recursive = True):
        pass
    
    
    def chmod(self, file_path, permission = '777', recursive = True):
        self.hdfs_client.chmod(file_path, permission)
        pass
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    