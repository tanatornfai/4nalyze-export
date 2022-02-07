import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

from datetime import datetime
from helper.database.db_task import db_export_job, db_execute_job, db_import_job, db_sensor_job, db_import_from_db_job, db_validate_records_job,export_to_s3_job
from helper.database.impala_task import impala_execute_job, impala_import_job, impala_sensor_job
from helper.database.rs_task import rs_export_job, rs_execute_job, rs_import_job, rs_sensor_job
from helper.utility.log_helper import log_helper 
from helper.utility.param_helper import *



def sql_task(parameter, **context):
    sql_job(parameter, context)
    
def sql_job(parameter, context):
    
    parameter = read_parameter(parameter, context)
    # DAG Parameter
    log_endpoint_id = parameter.get('LOG_ENDPOINT_ID', None)   
    
    
    start = datetime.now()
    try:
        endpoint_type = parameter['ENDPOINT_TYPE'].upper()
        if endpoint_type == 'DB':
            df_list = db_execute_job(parameter, context)
        elif endpoint_type in ['RS', 'REDSHIFT']:
            df_list = rs_execute_job(parameter, context)
        elif endpoint_type in ['IMPALA', 'KUDU']:
            df_list = impala_execute_job(parameter, context)
        else:
            raise Exception(f'Unknown Endpoint_type : {endpoint_type}')
            
        if log_endpoint_id is not None:
            end = datetime.now()
            print('=========================================')
            print('Insert Log')
            print('=========================================')
            log = log_helper(log_endpoint_id)
            log.insert_log(parameter, start, end, 'SUCCESS', '')

        return df_list
    
    except Exception as err:
        if log_endpoint_id is not None:
            end = datetime.now()
            print('=========================================')
            print('Insert Log')
            print('=========================================')
            log = log_helper(log_endpoint_id)
            log.insert_log(parameter, start, end, 'FAIL', str(err))
        raise err
        

def import_db_task(parameter, **context):
    import_db_job(parameter, context)

def import_db_job(parameter, context):
    
    parameter = read_parameter(parameter, context)
    # DAG Parameter
    log_endpoint_id = parameter.get('LOG_ENDPOINT_ID', None)   
    
    start = datetime.now()
    try:
        endpoint_type = parameter['ENDPOINT_TYPE'].upper()
        if endpoint_type == 'DB':
            db_import_job(parameter, context)
        elif endpoint_type in ['RS', 'REDSHIFT']:
            rs_import_job(parameter, context)
        elif endpoint_type in ['IMPALA', 'KUDU']:
            impala_import_job(parameter, context)
        else:
            raise Exception(f'Unknown Endpoint_type : {endpoint_type}')

        if log_endpoint_id is not None:
            end = datetime.now()
            print('=========================================')
            print('Insert Log')
            print('=========================================')
            log = log_helper(log_endpoint_id)
            log.insert_log(parameter, start, end, 'SUCCESS', '')
    
    except Exception as err:
        if log_endpoint_id is not None:
            end = datetime.now()
            print('=========================================')
            print('Insert Log')
            print('=========================================')
            log = log_helper(log_endpoint_id)
            log.insert_log(parameter, start, end, 'FAIL', str(err))
        raise err

        
def export_db_task(parameter, **context):
    export_db_job(parameter, context)       
        
def export_db_job(parameter, context):
    
    parameter = read_parameter(parameter, context)
    # DAG Parameter
    log_endpoint_id = parameter.get('LOG_ENDPOINT_ID', None)   
    
    start = datetime.now()
    try:
        endpoint_type = parameter['ENDPOINT_TYPE'].upper()
        if endpoint_type == 'DB':
            db_export_job(parameter, context)
        elif endpoint_type in ['RS', 'REDSHIFT']:
            rs_export_job(parameter, context)
        elif endpoint_type in ['IMPALA', 'KUDU']:
            pass
        else:
            raise Exception(f'Unknown Endpoint_type : {endpoint_type}')
     
        if log_endpoint_id is not None:
            end = datetime.now()
            print('=========================================')
            print('Insert Log')
            print('=========================================')
            log = log_helper(log_endpoint_id)
            log.insert_log(parameter, start, end, 'SUCCESS', '')
    
    except Exception as err:
        if log_endpoint_id is not None:
            end = datetime.now()
            print('=========================================')
            print('Insert Log')
            print('=========================================')
            log = log_helper(log_endpoint_id)
            log.insert_log(parameter, start, end, 'FAIL', str(err))
        raise err

        
def sensor_task(parameter, **context):
    sensor_job(parameter, context)
        
def sensor_job(parameter, context):
    
    parameter = read_parameter(parameter, context)
    # DAG Parameter
    log_endpoint_id = parameter.get('LOG_ENDPOINT_ID', None)   
    
    start = datetime.now()
    try:
        endpoint_type = parameter['ENDPOINT_TYPE'].upper()
        if endpoint_type == 'DB':
            db_sensor_job(parameter, context)
        elif endpoint_type in ['RS', 'REDSHIFT']:
            rs_sensor_job(parameter, context)
        elif endpoint_type in ['IMPALA', 'KUDU']:
            impala_sensor_job(parameter, context)
        else:
            raise Exception(f'Unknown Endpoint_type : {endpoint_type}')

        if log_endpoint_id is not None:
            end = datetime.now()
            print('=========================================')
            print('Insert Log')
            print('=========================================')
            log = log_helper(log_endpoint_id)
            log.insert_log(parameter, start, end, 'SUCCESS', '')

    except Exception as err:
        if log_endpoint_id is not None:
            end = datetime.now()
            print('=========================================')
            print('Insert Log')
            print('=========================================')
            log = log_helper(log_endpoint_id)
            log.insert_log(parameter, start, end, 'FAIL', str(err))
        raise err

def import_from_db_task(parameter, **context):
    import_from_db_job(parameter, context)        
        
def import_from_db_job(parameter, context):
    
    parameter = read_parameter(parameter, context)
    # DAG Parameter
    log_endpoint_id = parameter.get('LOG_ENDPOINT_ID', None)   
    
    start = datetime.now()
    try:
        endpoint_type = parameter['ENDPOINT_TYPE'].upper()
        if endpoint_type == 'DB':
            db_import_from_db_job(parameter, context)
        elif endpoint_type in ['RS', 'REDSHIFT']:
            raise Exception(f'Not Support Endpoint_type : {endpoint_type}')
        elif endpoint_type in ['IMPALA', 'KUDU']:
            raise Exception(f'Not Support Endpoint_type : {endpoint_type}')
        else:
            raise Exception(f'Unknown Endpoint_type : {endpoint_type}')

        if log_endpoint_id is not None:
            end = datetime.now()
            print('=========================================')
            print('Insert Log')
            print('=========================================')
            log = log_helper(log_endpoint_id)
            log.insert_log(parameter, start, end, 'SUCCESS', '')

    except Exception as err:
        if log_endpoint_id is not None:
            end = datetime.now()
            print('=========================================')
            print('Insert Log')
            print('=========================================')
            log = log_helper(log_endpoint_id)
            log.insert_log(parameter, start, end, 'FAIL', str(err))
        raise err

def validate_records_task(parameter, **context):
    validate_records_job(parameter, context)        
        
def validate_records_job(parameter, context):
    
    parameter = read_parameter(parameter, context)
    # DAG Parameter
    log_endpoint_id = parameter.get('LOG_ENDPOINT_ID', None)   
    
    start = datetime.now()
    try:
        endpoint_type = parameter['ENDPOINT_TYPE'].upper()
        if endpoint_type == 'DB':
            db_validate_records_job(parameter, context)
        elif endpoint_type in ['RS', 'REDSHIFT']:
            raise Exception(f'Not Support Endpoint_type : {endpoint_type}')
        elif endpoint_type in ['IMPALA', 'KUDU']:
            raise Exception(f'Not Support Endpoint_type : {endpoint_type}')
        else:
            raise Exception(f'Unknown Endpoint_type : {endpoint_type}')

        if log_endpoint_id is not None:
            end = datetime.now()
            print('=========================================')
            print('Insert Log')
            print('=========================================')
            log = log_helper(log_endpoint_id)
            log.insert_log(parameter, start, end, 'SUCCESS', '')

    except Exception as err:
        if log_endpoint_id is not None:
            end = datetime.now()
            print('=========================================')
            print('Insert Log')
            print('=========================================')
            log = log_helper(log_endpoint_id)
            log.insert_log(parameter, start, end, 'FAIL', str(err))
        raise err
def export_to_s3_task(parameter,**context):
    parameter = read_parameter(parameter, context)
    export_to_s3_job(parameter,context)