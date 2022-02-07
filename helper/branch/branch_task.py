import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

from datetime import datetime
from helper.database.sql_task import sql_job
from helper.utility.param_helper import *
from helper.utility.log_helper import log_helper 



def branch_task(parameter, **context):
    
    parameter = read_parameter(parameter, context)
    # DAG Parameter
    log_endpoint_id = parameter.get('LOG_ENDPOINT_ID', None) 
    
    start = datetime.now()
    try:
        # Required parameter
        key_dict = parameter.get('KEY_DICT', None)
        if key_dict == None:
            raise Exception("Need XCOM key")
        
        return_task = None
        for xcom_key in key_dict.keys():
            if xcom_key != 'UNMATCH_TASK':
                key_split = xcom_key.split('.')
                if len(key_split) > 4: # 4
                    dag_name = key_split[0]
                    task = '.'.join(key_split[1:-1])
                    key = key_split[-1]
                elif len(key_split) == 4: #4
                    dag_name = context['dag'].dag_id
                    task = '.'.join(key_split[0:-1]) 
                    key = key_split[-1]
                print(f"Pulling XCOM | key : {key} | task_id : {task} | dag_di {dag_name} | Execution date : {context['execution_date']}")
                value = context['ti'].xcom_pull(key = key, task_ids = task, dag_id = dag_name ,include_prior_dates = True)
                print(f"Get value : {value}")
                compare_type = key_dict[xcom_key]['COMPARE_TYPE'].upper()
                expect_value = key_dict[xcom_key]['EXPECTED_VALUE']
                if value != None:
                    if compare_type == 'EXACT':
                        if value == expect_value:
                            print('EXACT')
                            return_task = key_dict[xcom_key]['MATCH_TASK']
                            break
                    elif compare_type == 'GREATER':
                        if value > expect_value:
                            print('GREATER')
                            return_task = key_dict[xcom_key]['MATCH_TASK']
                            break
                    elif compare_type == 'LESSER':
                        if value < expect_value:
                            print('LESSER')
                            return_task = key_dict[xcom_key]['MATCH_TASK']
                            break
                    else:
                        raise Exception(f"Invalid COMPARE_TYPE {key_dict[xcom_key]['COMPARE_TYPE']} doesn't existed")
            elif xcom_key == 'UNMATCH_TASK':
                return_task = key_dict['UNMATCH_TASK']
                break
                                        
        if log_endpoint_id is not None:
            end = datetime.now()
            print('=========================================')
            print('Insert Log')
            print('=========================================')
            log = log_helper(log_endpoint_id)
            log.insert_log(parameter, start, end, 'SUCCESS', '')
           
        return return_task
                                        
    except Exception as err:
        if log_endpoint_id is not None:
            end = datetime.now()
            print('=========================================')
            print('Insert Log')
            print('=========================================')
            log = log_helper(log_endpoint_id)
            log.insert_log(parameter, start, end, 'FAIL', str(err))
        raise err