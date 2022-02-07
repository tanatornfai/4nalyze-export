import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

import re
from helper.file.file_helper import file_helper

user = os.getenv('USER')
endpoint_dict = dict()
_root_folder = root_path
_credential_folder = f'{_root_folder}credential/config/'
# _dag_conf_folder = f'{_root_folder}dag/{user}/config/'
_script_folder = f'{_root_folder}script/'


####################################################################
## FILE INFORMATION
####################################################################
def get_file_infor(file_path):
    file_name = file_path.split('/')[-1]
    folder_path = os.path.dirname(file_path)
    
    return file_path, file_name, folder_path


def get_obj(path):
    match = re.search('\w+://', path)

    if match:
        endpoint_type = match.group()[:-3].upper()
        obj_path = path[match.span()[1]:]
        
        if endpoint_type == 'FILE':
            endpoint_type = 'FILE'
            endpoint_id = 'FILE'
            obj_path = path
            
        elif endpoint_type == 'S3':
            endpoint_id, obj_path = obj_path.split('/', 1)
            
        elif endpoint_type == 'HDFS':
            endpoint_id = 'HDFS'
        
        elif endpoint_type == 'REMOTE':
            endpoint_id, obj_path = obj_path.split('/', 1)

        elif endpoint_type == 'SFTP':
            endpoint_id, obj_path = obj_path.split('/', 1)

        else:
            raise Exception(f'Unknown Endpoint Type : {endpoint_type} - {path}')
    else:
        endpoint_type = 'FILE'
        endpoint_id = 'FILE'
        obj_path = path
    
    return endpoint_type, endpoint_id, obj_path

####################################################################
## ENDPOINT
####################################################################
def get_endpoint_id_list():
    return endpoint_dict.keys()


def get_endpoint(endpoint_type, endpoint_id):
    return endpoint_dict[(endpoint_type.upper(), endpoint_id)]


def set_endpoint(endpoint, endpoint_type, endpoint_id):
    endpoint_dict[(endpoint_type, endpoint_id)] = endpoint
    

def create_endpoint(endpoint_type, endpoint_id):
    if endpoint_type == 'FILE':
        endpoint = file_helper()
        
    elif endpoint_type == 'S3':
        from helper.file.s3_helper import s3_helper
        endpoint = s3_helper(endpoint_id, authentication=True)
        
    elif endpoint_type == 'HDFS':
        from helper.file.hdfs_helper import hdfs_helper
        endpoint = hdfs_helper(endpoint_id)
        
    elif endpoint_type == 'SFTP':
        from helper.file.sftp_helper import sftp_helper
        endpoint = sftp_helper(endpoint_id)
        
    elif endpoint_type == 'REMOTE':
        from helper.shell.ssh_helper import ssh_helper
        endpoint = ssh_helper(endpoint_id)
        
    else:
        raise Exception(f'Unknown Endpoint Type : {endpoint_type}')
    
    set_endpoint(endpoint, endpoint_type, endpoint_id)
    return endpoint


def prepare_endpoint(endpoint_type, endpoint_id):
    if (endpoint_type, endpoint_id) not in get_endpoint_id_list():
        endpoint = create_endpoint(endpoint_type, endpoint_id)
    else:
        endpoint = get_endpoint(endpoint_type, endpoint_id)
    return endpoint



####################################################################
## READ
####################################################################
def read_file(file_path, print_log=True):
    endpoint_type, endpoint_id, obj_path = get_obj(file_path)
    
    endpoint = prepare_endpoint(endpoint_type, endpoint_id)
    
    content = endpoint.read_file(obj_path, print_log=print_log)
    return content


def read_dag_conf(project_id, project_name, dag_name,user):
    _dag_conf_folder = f'{_root_folder}task/{user}/config/'
    dag_conf_df = read_file(f'{_dag_conf_folder}{project_id}/{project_name} - dag_config.csv')
    dag_conf_df = dag_conf_df[dag_conf_df['dag_name'].str.upper() == dag_name.upper()]
    if len(dag_conf_df) == 1:
        return dag_conf_df.iloc[0]
    else:
        raise Exception(f'Dag {dag_name} configuration not found in {project_id}')


def read_zone_conf(project_id, project_name, dag_name, zone_name,user):
    _dag_conf_folder = f'{_root_folder}task/{user}/config/'
    task_conf_df = read_file(f'{_dag_conf_folder}{project_id}/{dag_name}/{project_name} - {dag_name}_{zone_name}_task.csv')
    relation_conf_df = read_file(f'{_dag_conf_folder}{project_id}/{dag_name}/{project_name} - {dag_name}_{zone_name}_relation.csv')
    return task_conf_df, relation_conf_df


####################################################################
## LIST
####################################################################
def list_file(file_path, show_log = True):
    endpoint_type, endpoint_id, obj_path = get_obj(file_path)
    endpoint = prepare_endpoint(endpoint_type, endpoint_id)
    
    content = endpoint.list_file(obj_path, show_log)
    return content
    

####################################################################
## MANAGE - DELETE, MOVE, COPY
####################################################################
def delete_file(file_path, show_log = True):
    endpoint_type, endpoint_id, obj_path = get_obj(file_path)
    endpoint = prepare_endpoint(endpoint_type, endpoint_id)
    
    content = endpoint.delete_file(obj_path, show_log)
    return content


def clear_file(file_path, show_log = True):
    content = delete_file(file_path, show_log)
    return content


def move_file(source_path, destination_path, clear_destination = True, show_log = True):
    endpoint_type1, endpoint_id1, source_path = get_obj(source_path)
    endpoint_type2, endpoint_id2, destination_path = get_obj(destination_path)
    if endpoint_type1 != endpoint_type2 or endpoint_id1 != endpoint_id2:
        raise Exception(
            f'''Endpoint in Source and Destination is different, 
    Source Endpoint : {endpoint_type1},{endpoint_id1}
    Destination Endpoint : {endpoint_type2},{endpoint_id2}''')
    endpoint = prepare_endpoint(endpoint_type1, endpoint_id1)
    
    content = endpoint.move_file(source_path, destination_path, clear_destination, show_log)
    return content


def copy_file(source_path, destination_path, clear_destination = True, show_log = True):
    endpoint_type1, endpoint_id1, source_path = get_obj(source_path)
    endpoint_type2, endpoint_id2, destination_path = get_obj(destination_path)
    if endpoint_type1 != endpoint_type2 or endpoint_id1 != endpoint_id2:
        raise Exception(
            f'''Endpoint in Source and Destination is different, 
    Source Endpoint : {endpoint_type1},{endpoint_id1}
    Destination Endpoint : {endpoint_type2},{endpoint_id2}''')
    endpoint = prepare_endpoint(endpoint_type1, endpoint_id1)
    
    content = endpoint.copy_file(source_path, destination_path, clear_destination, show_log)
    return content

####################################################################
## Count row
####################################################################
def count_rows(file_path, show_log = True):
    endpoint_type, endpoint_id, obj_path = get_obj(file_path)
    endpoint = prepare_endpoint(endpoint_type, endpoint_id)
    
    content = endpoint.count_rows(obj_path, show_log)
    return content

####################################################################
## DOWNLOAD & UPLOAD
####################################################################
def download_file(source_path, destination_path, clear_destination = True, show_log = True):
    endpoint_type1, endpoint_id1, source_path = get_obj(source_path)
    endpoint_type2, endpoint_id2, destination_path = get_obj(destination_path)
    
    if endpoint_type2 != 'FILE':
        raise Exception(f'Download to Endpoint : {endpoint_type2} not support, please change endpoint to "FILE"')
    endpoint = prepare_endpoint(endpoint_type1, endpoint_id1)

    content = endpoint.download_file(source_path, destination_path, clear_destination, show_log)


def upload_file(source_path, destination_path, clear_destination = True, show_log = True):
    endpoint_type1, endpoint_id1, source_path = get_obj(source_path)
    endpoint_type2, endpoint_id2, destination_path = get_obj(destination_path)
    
    if endpoint_type1 != 'FILE':
        raise Exception(f'Upload from Endpoint : {endpoint_type1} not support, please change endpoint to "FILE"')
    endpoint = prepare_endpoint(endpoint_type2, endpoint_id2)

    content = endpoint.upload_file(source_path, destination_path, clear_destination, show_log)

    
    
####################################################################
## UTILITY
####################################################################
def ensure_dir(file_path, show_log = True):
    endpoint_type, endpoint_id, obj_path = get_obj(file_path)
    endpoint = prepare_endpoint(endpoint_type, endpoint_id)
    
    content = endpoint.ensure_dir(obj_path, show_log)
    return content

def create_dir(dir_path, show_log = True):
    endpoint_type, endpoint_id, obj_path = get_obj(dir_path)
    endpoint = prepare_endpoint(endpoint_type, endpoint_id)
    
    content = endpoint.create_dir(obj_path, show_log)
    return content

def check_file_exists(file_path, show_log = True):
    endpoint_type, endpoint_id, obj_path = get_obj(file_path)
    endpoint = prepare_endpoint(endpoint_type, endpoint_id)
    
    content = endpoint.check_file_exists(obj_path, show_log)
    return content

def check_dir(file_path, show_log = True):
    endpoint_type, endpoint_id, obj_path = get_obj(file_path)
    endpoint = prepare_endpoint(endpoint_type, endpoint_id)
    
    content = endpoint.check_dir(obj_path, show_log)
    return content

####################################################################
## CREDENTIAL
####################################################################
def get_credential(endpoint_type, endpoint_id, validate_endpoint=True, print_log=True):
    # endpoint_type = db, s3, ssh, ftp, hdfs
    config_path = f'{_credential_folder}Airflow_Framework - {endpoint_type.lower()}_config.csv'
    df = read_file(config_path, print_log=print_log)
    df = df[df['endpoint_id'] == endpoint_id]

    if validate_endpoint:
        if len(df) == 0:
            raise Exception(f"Endpoint {endpoint_type} - {endpoint_id}'s credential Not Found")
        if len(df) > 1:
            raise Exception(f'Multiple Endpoint {endpoint_type} - {endpoint_id} are detected, Please revise endpoint configuration')
    else:
        if len(df) == 0:
            return dict()
        
    parameter = dict(df.iloc[0])
    parameter = {k: v for k, v in parameter.items() if v is not None}
    return parameter
    