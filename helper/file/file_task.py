import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

from helper.file import file_manager
from helper.utility.param_helper import read_parameter



def delete_task(parameter, **context):
    delete_job(parameter, context)
        
def delete_job(parameter, context):
    parameter = read_parameter(parameter, context)
    
    # Required parameter
    file_path = parameter['FILE_PATH']

    # Optional parameter
    recursive = parameter.get('RECURSIVE', True)
    
    print('=========================================')
    print('Create Connection')
    print('=========================================')
    xxx = file_manager(xxxx)
    
    
    
    
    