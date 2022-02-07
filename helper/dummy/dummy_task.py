import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

from helper.utility.param_helper import read_parameter
from helper.utility.log_helper import log_helper 

import json
import pandas as pd

import time
from datetime import datetime



########################################################
# Dummy Python
########################################################
def dummy_task(parameter, **context):
    dummy_python_job(parameter, context)

def dummy_python_task(parameter, **context):
    dummy_python_job(parameter, context)
        
def dummy_python_job(parameter, context):
   
    parameter = read_parameter(parameter, context)
    # DAG Parameter
    log_endpoint_id = parameter.get('LOG_ENDPOINT_ID', None)  
    
    start = datetime.now()
    try:
        print(f'CONTEXT \n')
        for key in context.keys():
            print(f'key : {context[key]}')

        delay = int(parameter.get('delay', 5))
        for i in range(delay):
            print(f'delay : {i}')
            time.sleep(1)

        status = parameter.get('status', 'SUCCESS')
        if status != 'SUCCESS':
            raise Exception(status)
        
        if log_endpoint_id is not None:
            end = datetime.now()
            print('=========================================')
            print('Insert Log')
            print('=========================================')
            log = log_helper(log_endpoint_id)
            log.insert_log(parameter, start, end, 'SUCCESS', '')
            
        return status
    
    except Exception as err:
        if log_endpoint_id is not None:
            end = datetime.now()
            print('=========================================')
            print('Insert Log')
            print('=========================================')
            log = log_helper(log_endpoint_id)
            log.insert_log(parameter, start, end, 'FAIL', str(err))
        raise err
    
    
########################################################
# Dummy Branch
########################################################
def dummy_branch_task(parameter, **context):
    dummy_branch_job(parameter, context)
    
def dummy_branch_job(parameter, context):
    
    parameter = read_parameter(parameter, context)
    # DAG Parameter
    log_endpoint_id = parameter.get('LOG_ENDPOINT_ID', None)  
    
    start = datetime.now()
    try:
        # Required parameter
        next_task_id = parameter['NEXT_TASK_ID']
        print(f'Next Task ID : {next_task_id}')
        
        if log_endpoint_id is not None:
            end = datetime.now()
            print('=========================================')
            print('Insert Log')
            print('=========================================')
            log = log_helper(log_endpoint_id)
            log.insert_log(parameter, start, end, 'SUCCESS', '')

        return next_task_id
    
    except Exception as err:
        if log_endpoint_id is not None:
            end = datetime.now()
            print('=========================================')
            print('Insert Log')
            print('=========================================')
            log = log_helper(log_endpoint_id)
            log.insert_log(parameter, start, end, 'FAIL', str(err))
        raise err