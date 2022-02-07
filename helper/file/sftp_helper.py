import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

import io
import stat
# import pysftp
import paramiko
from io import BytesIO ,StringIO
import pandas as pd

from helper.file import file_manager



class sftp_helper:
    endpoint_type = 'SFTP'
    hostname = None
    username = None
    sftp = None
    default_path = None

    def __init__(self, endpoint_id):
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
        port = row.get('port', 22)
        self.default_path = row.get('default_path', '/')
        
        self.username = row['username']
        password = row.get('password', None)
        private_key_path = row.get('private_key_path', None)
        passphrase = row.get('passphrase', None)

        if self.hostname is None:
            if ipaddress is None:
                raise Exception('Both hostname and ipaddress is empty')
            self.hostname = ipaddress 
            
        if password is None and private_key_path is None:
            raise Exception('Both password and private_key_path is empty')

        print('Creating host key')
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        print(f'\tConnecting to {self.endpoint_type} {self.username} @ {self.hostname}:{port}')
        if private_key_path is not None:
            print(f'Found private_key')
            private_key = file_manager.read_file(private_key_path)
            private_key = io.StringIO(private_key)
            
            if passphrase is not None:
                pkey = paramiko.RSAKey.from_private_key(private_key, passphrase)
            else:
                pkey = paramiko.RSAKey.from_private_key(private_key)
            
            self.sftp = pysftp.Connection(
                self.hostname, 
                username = self.username, 
                private_key = pkey,
                port = port,
                cnopts=cnopts
            )
        elif password is not None:
            print('Found password')
            self.sftp = pysftp.Connection(
                self.hostname, 
                username = self.username, 
                password=password, 
                port = port,
                cnopts=cnopts
            )
        else:
            raise Exception(f'Both Password and Passphrase not found for User "{self.username}" on Host "{self.hostname}"')

        print(f'Create {self.endpoint_type} {endpoint_id} Connection Successfully\n\n')
        
        
        
    ####################################################################
    ## READ
    ####################################################################
    def read_csv(self, file_path, config_dict = dict()):
        file_path = self.replace_default_path(file_path)
        print(file_path)
        file_object = BytesIO()
        self.sftp.getfo(file_path,file_object)
        s=str(file_object.getvalue(),'utf-8')
        data = StringIO(s) 
        df=pd.read_csv(data)
        return df
        
        
    def read_excel(self, file_path, config_dict = dict()):
        file_path = self.replace_default_path(file_path)
        
        print(file_path)
        file_object = BytesIO()
        self.sftp.getfo(file_path,file_object)
        df = pd.read_excel(file_object.getvalue())
        return df
        
    def read_json(self, file_path, config_dict = dict()):
        file_path = self.replace_default_path(file_path)
        
        print(file_path)
        file_object = BytesIO()
        self.sftp.getfo(file_path,file_object)
        
        s=str(file_object.getvalue(),'utf-8')
        data = StringIO(s) 
        df = pd.read_json(data,lines=True)
        return df
        
    def read_parquet(self, file_path, config_dict = dict()):
        file_path = self.replace_default_path(file_path)
        print(file_path)
        
        file_object = BytesIO()
        self.sftp.getfo(file_path,file_object)
        df = pd.read_parquet(file_object)
        return df
    
    def read_text(self, file_path, config_dict = dict()):
        file_path = self.replace_default_path(file_path)
        print(file_path)
        
        file_object = BytesIO()
        self.sftp.getfo(file_path,file_object)
        s=str(file_object.getvalue(),'utf-8')
        return s
    
    
    def read_lines(self, file_path, config_dict = dict()):
        file_path = self.replace_default_path(file_path)
        file_object = BytesIO()
        self.sftp.getfo(file_path,file_object)
        s=str(file_object.getvalue(),'utf-8')
        return s.splitlines()
    
    def read_file(self, file_path, config_dict = dict()):
        file_path = self.replace_default_path(file_path)
        
        if file_path.endswith('.csv'):
            content = self.read_csv(file_path, config_dict)
            
        elif file_path.endswith('.xlsx') or file_path.endswith('.xls'):
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
        file_path = self.replace_default_path(file_path)
        
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
            obj_list = [(obj,filename) for obj,filename in obj_list if obj.longname[0] != 'd']
        elif include == 'dir':
            obj_list = [(obj,filename) for obj,filename in obj_list if obj.longname[0] == 'd']
        
        
        if show_log:
            print(f' - Count : {len(obj_list)} files')
            for obj, filename in obj_list:
                print(f'\t{filename}')
        
        if return_obj:
            return obj_list
        else:
            file_list = [filename for obj,filename in obj_list]
            return file_list
        
        
    def list_obj(self, file_path, show_log = True, return_obj = False):
        if len(file_path) > 0 and file_path[-1] == '/':
            file_path = file_path[:-1]

        obj_list = []
        this_obj_list = self.sftp.listdir_attr(remotepath = file_path)

        for obj in this_obj_list:
            this_file_path = f'{file_path}/{obj.filename}'
            if obj.longname[0] == 'd':
                sub_obj_list = self.list_obj(this_file_path, show_log, return_obj)
                obj_list = obj_list + sub_obj_list
                obj_list.append([obj, f'{this_file_path}/'])
            else:
                obj_list.append([obj, this_file_path])

        return obj_list
        
    
        
    ####################################################################
    ## MANAGE - DELETE, MOVE, COPY
    ####################################################################   
    def delete_dir(self, dir_path):
        self.sftp.rmdir(remotefile = dir_path)
    
    
    def delete(self, file_path):
        self.sftp.remove(remotefile = file_path)
    
    
    def delete_file(self, file_path, show_log = True):
        file_path = self.replace_default_path(file_path)
        
        print(f'Deleting {self.endpoint_type} path :')
        print(f' - Path : {file_path}')
        
        file_list = self.list_file(file_path, show_log = False, include = 'file')
        folder_list = self.list_file(file_path, show_log = False, include = 'dir')
        
        # delete file
        for file_path in file_list:
            print(f'\tDeleting {file_path}')
            self.delete(file_path)
            
        # delete folder
        for file_path in folder_list:
            print(f'\tDeleting {file_path}')
            self.delete_dir(file_path)
        
    
    def move_file(self, source_path, destination_path, clear_destination = True, show_log = True):
        source_path = self.replace_default_path(source_path)
        destination_path = self.replace_default_path(destination_path)
        
        print('Not Implement yet :P')
        pass
    
    
    def copy_file(self, source_path, destination_path, clear_destination = True, show_log = True):
        source_path = self.replace_default_path(source_path)
        destination_path = self.replace_default_path(destination_path)
        
        print('Not Implement yet :P')
        pass
    
        
        
    ####################################################################
    ## DOWNLOAD & UPLOAD
    ####################################################################
    def download(self, source_file_path, destination_file_path):
        self.sftp.get(
            remotepath = source_file_path, 
            localpath = destination_file_path, 
            preserve_mtime = False
        )
    
    def download_file(self, source_path, destination_path, clear_destination = True, show_log = True):
        source_path = self.replace_default_path(source_path)
        
        if clear_destination:
            print(f'Clearing Destination (local) :')
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
        print(f'Destination path (local) : ')
        file_list = file_manager.list_file(destination_path, show_log)
        print()
    
    
    def upload(self, source_file_path, destination_file_path):
        self.sftp.put(
            remotepath = destination_file_path, 
            localpath = source_file_path,
            confirm = True
        )
    
    
    def upload_file(self, source_path, destination_path, clear_destination = True, show_log = True): 
        destination_path = self.replace_default_path(destination_path)
        
        if clear_destination:
            print(f'Clearing Destination ({self.endpoint_type}) :')
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
    
    
    def get_object(self, source_path, destination_path, clear_destination = True, show_log = True):
        'alias of download_object'
        self.download_file(source_path, destination_path, clear_destination, show_log)
        
        
    def put_object(self, source_path, destination_path, clear_destination = True, show_log = True):
        'alias of upload_object'
        self.upload_file(source_path, destination_path, clear_destination, show_log)
    
    
    
    ####################################################################
    ## UTILITY
    ####################################################################
    def get_file_size(self, file_path):
        file_path = self.replace_default_path(file_path)
        
        print('Not Implement yet :P')
        pass
    
    
    def is_dir(self, file_path):
        fileattr = self.sftp.stat(file_path)
        is_dir = stat.S_ISDIR(fileattr.st_mode)
        return is_dir
    
    
    def check_file_exists(self, file_path):
        file_path = self.replace_default_path(file_path)
        
        try:
            self.sftp.stat(file_path)
            return True
        except IOError as e:
            return False
    
    
    def ensure_dir(self, file_path, show_log = True):
        file_path = self.replace_default_path(file_path)
        
        dir_path = os.path.dirname(file_path)
        if not self.check_file_exists(dir_path):
            self.create_dir(dir_path, show_log)
        
        
    def create_dir(self, dir_path, show_log = True):
        file_path = self.replace_default_path(file_path)
        
        print(f'Creating dir : {dir_path}')
        self.sftp.makedirs(remotedir = dir_path)
        
    
    def replace_default_path(self, file_path):
        file_path = file_path.replace('{SFTP_DEFAULT_PATH}',self.default_path)
        return file_path
    