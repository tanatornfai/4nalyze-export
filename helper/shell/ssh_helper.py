import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

import io
import pytz
import paramiko
import fnmatch
import re
import subprocess

from helper.file import file_manager


class ssh_helper:
    endpoint_type = 'SSH'
    hostname = None
    username = None
    password = None
    ssh_client = None
    sftp = None
    def __init__(self, endpoint_id, print_log=True):
        self.create_con(endpoint_id, print_log=print_log)
    
    
    
    ##########################################
    # Create Connection
    ##########################################
    def create_con(self, endpoint_id, print_log=True):
        if print_log is True:
            print(f'Creating {self.endpoint_type.upper()} {endpoint_id} Endpoint')
        
        if print_log is True:
            print('\tGetting Credential')
        row = file_manager.get_credential(self.endpoint_type, endpoint_id, validate_endpoint = True, print_log=False)
        self.hostname = row.get('hostname', None)
        ipaddress = row.get('ipaddress', None)
        port = int(row.get('port', 22))
        
        self.username = row['username']
        self.password = row.get('password', None)
        private_key_path = row.get('private_key_path', None)
        passphrase = row.get('passphrase', None)

        if self.hostname is None:
            if ipaddress is None:
                raise Exception('Both hostname and ipaddress is empty')
            self.hostname = ipaddress
            
        if self.password is None and private_key_path is None:
            raise Exception('Both password and private_key_path is empty')

        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        if print_log is True:
            print(f'\tConnecting to {self.endpoint_type} {self.username} @ {self.hostname}:{port}')
        if private_key_path is not None:
            if print_log is True:
                print(f'Found private_key')
            private_key = file_manager.read_file(private_key_path)
            private_key = io.StringIO(private_key)
            
            if passphrase is not None:
                pkey = paramiko.RSAKey.from_private_key(private_key, passphrase)
            else:
                pkey = paramiko.RSAKey.from_private_key(private_key)
            
            self.ssh_client.connect(
                hostname = self.hostname, 
                username = self.username, 
                pkey = pkey,
                port = port
            )
        elif self.password is not None:
            if print_log is True:
                print('Found password')
            self.ssh_client.connect(
                hostname = self.hostname, 
                username = self.username, 
                password = self.password,
                port = port
            )
        else:
            raise Exception(f'Both Password and Passphrase not found for User "{self.username}" on Host "{self.hostname}"')
        self.sftp = self.ssh_client.open_sftp()
        if print_log is True:
            print(f'Create {self.endpoint_type} {endpoint_id} Connection Successfully\n\n')
    
    ##########################################
    # Execute script
    ##########################################
    def send_command(self, command, check_returncode = True, print_log=True, return_log=False, password=None):
        log = ''
        if print_log is True:
            print(f'Executing Script on {self.hostname}')
        if return_log is True:
            log += f'Executing Script on {self.hostname}\n'
        
        stdin, stdout, stderr = self.ssh_client.exec_command(command)
        
        if password is not None:
            stdin.write(password + '\n')
            stdin.flush()
        
        return_code = stdout.channel.recv_exit_status()
        stdout = ''.join(stdout.readlines())
        stderr = ''.join(stderr.readlines())
        
        if return_code != 0:
            if print_log is True:
                print(f'Shell Script Error !!!')
            if return_log is True:
                log += f'Shell Script Error !!!\n'
            if check_returncode:
                raise Exception(f'Run Shell Script Error - return code : {return_code}')

        else:
            if print_log is True:
                print('\tRun Script Successfully')
            if return_log is True:
                log += f'\tRun Script Successfully\n'
            
        if return_log is True:
            return return_code, stdout, stderr, log
        
        return return_code, stdout, stderr
    
    ##########################################
    # File management
    ##########################################
    def list_file(self,file_path, show_log = True, return_obj = False, include = 'file'):
        print(f'Listing file : ')
        print(f' - File_path : {file_path}')
        obj_list = self.list_obj(file_path, show_log)

        if include == 'file':
            obj_list = [obj for obj in obj_list if '.' in obj ]
        elif include == 'dir':
            obj_list = [obj for obj in obj_list if '.' not in obj]

        if return_obj:
            return obj_list
        else:
            file_list = [obj for obj in obj_list]
            return file_list


    def list_obj(self,file_path, show_log = True):
        file_path_list = file_path.rsplit('/',1)
        if '.' in file_path_list[-1]:
            file_path, file_name = file_path_list
        else:
            file_name = ''
        if file_path[-1] == '/':
            file_path = file_path[:-1]

        obj_list = [f'{file_path}/{obj}' for obj in self.sftp.listdir(file_path)]
        if file_name != '':
            obj_list = [obj for obj in obj_list if fnmatch.fnmatchcase(obj, f'*{file_name}')]

        if show_log:
            print(f' - File_name : {file_name}')
            print(f' - Count : {len(obj_list)} files')
            for obj in obj_list:
                print(f'\t{obj}')

        return obj_list
              
    def download_file(self, source_path, destination_path, clear_destination = False, show_log = True):
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
            destination_file_path = source_file_path.replace(folder_path, destination_path, 1)

            file_manager.ensure_dir(destination_file_path)

            print(f'\t{file_name}')
            print(f'\t - from {source_file_path}')
            print(f'\t - to {destination_file_path}')
            self.sftp.get(source_file_path, destination_file_path)
        print()

        # Check Destination Path
        print(f'Destination path (local): ')
        file_list = file_manager.list_file(destination_path, show_log)
        print()

    def delete_file(self, file_path, show_log = True):
        command = f'rm -r {file_path}; mkdir {file_path};'
        return_code, stdout, stderr = self.send_command(command, check_returncode = True)
        return stdout
    
    def check_dir(self, file_path, show_log = True):
        command = f'if [ ! -d {file_path} ]; then mkdir -p {file_path}; fi'
        return_code, stdout, stderr = self.send_command(command, check_returncode = True)
        return stdout
    def create_dir(self,dir_path,show_log = True):
        stdout = self.check_dir(dir_path, show_log = show_log)
        return stdout
    def count_rows(self, file_path, show_log = True):
        command = f'cat {file_path}/* | wc -l'
        return_code, stdout, stderr = self.send_command(command, check_returncode = True)
        return stdout