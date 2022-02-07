import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

import json
import pytz

import numpy as np

from pathlib import Path
from datetime import datetime, timedelta

# Operator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
# from airflow.operators.mysql_operator import MySqlOperator
# from airflow.hooks.mysql_hook import MySqlHook

# from airflow.providers.papermill.operators.papermill import PapermillOperator

# Sensor
from airflow.sensors.external_task_sensor import ExternalTaskSensor

# Dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Email Callback
from helper.email.email_helper import custom_email_callback

from helper.utility.time_helper import get_process_date
from helper.utility.param_helper import replace_params_intext



########################################################
# Operator
########################################################
def create_python_task(task_id, dag, params, function_id, 
                       dag_retry_count = 3, dag_retry_delay = 5, dag_email_list = [], dag_mail_on_success = False, task_log = ''):
    function = get_function(function_id)
    
    retry_count = params.get('retry_count', dag_retry_count)
    retry_delay = params.get('retry_delay', dag_retry_delay)
    email_list = params.get('email_list', dag_email_list)
    mail_on_success = params.get('mail_on_success', dag_mail_on_success)
    task_log = params.get('task_log', task_log)
    
    task = PythonOperator(
        task_id = task_id,
        python_callable = function,
        op_kwargs = params,
        dag = dag,
        provide_context = True,
        trigger_rule = 'none_failed_or_skipped',
        retries = retry_count,
        retry_delay = timedelta(minutes = retry_delay),
        **custom_email_callback(email_list, task_log = task_log, attachment_files = '', on_success = mail_on_success)
    )
    return task    


def create_branch_task(task_id, dag, params, function_id,
                      dag_retry_count = 3, dag_retry_delay = 5, dag_email_list = [], dag_mail_on_success = False, task_log = ''):
    function = get_function(function_id)
    
    retry_count = params.get('retry_count', dag_retry_count)
    retry_delay = params.get('retry_delay', dag_retry_delay)
    email_list = params.get('email_list', dag_email_list)
    mail_on_success = params.get('mail_on_success', dag_mail_on_success)
    task_log = params.get('task_log', task_log)
    
    task = BranchPythonOperator(
        task_id = task_id,
        python_callable = function,
        op_kwargs = params,
        provide_context = True,
        dag = dag,
        trigger_rule = 'none_failed_or_skipped',
        retries = retry_count,
        retry_delay = timedelta(minutes = retry_delay),
        **custom_email_callback(email_list, task_log = task_log, attachment_files = '', on_success = mail_on_success)
    )
    return task

# def create_notebook_task(task_id, dag, params,
#                          dag_retry_count = 3, dag_retry_delay = 5, dag_email_list = [], dag_mail_on_success = False, task_log = ''):
#     notebook_name = params['parameter']['NOTEBOOK_NAME'].replace('.ipynb','')
#     notebook_name = replace_params_intext(notebook_name, params['parameter'])
    
#     input_dir = params['parameter']['NOTEBOOK_DIR']
#     input_dir = replace_params_intext(input_dir, params['parameter'])
#     save_log = params['parameter'].get('SAVE_LOG', True)
    
#     project_id = params['parameter']['PROJECT_ID']
#     dag_name = params['parameter']['DAG_NAME']
#     zone_name = params['parameter']['ZONE_NAME']
#     job_name = params['parameter']['JOB_NAME']
    
#     output_dir = f'/usr/local/airflow/log/{project_id}/{dag_name}/{zone_name}/{job_name}/'
#     Path(output_dir).mkdir(parents=True, exist_ok=True)
    
#     if input_dir[-1] != '/':
#         input_dir += '/'
#     input_nb = f'{input_dir}{notebook_name}.ipynb'
#     output_nb = f'{output_dir}{notebook_name}_{get_process_date()}.ipynb'
    
#     retry_count = params.get('retry_count', dag_retry_count)
#     retry_delay = params.get('retry_delay', dag_retry_delay)
#     email_list = params.get('email_list', dag_email_list)
#     mail_on_success = params.get('mail_on_success', dag_mail_on_success)
#     task_log = params.get('task_log', task_log)
    
#     params['parameter']['PROCESS_DATE'] = '{{ next_execution_date }}'
#     params['parameter']['EXECUTION_DATE'] = '{{ execution_date }}'
     
#     if save_log:
#         task = PapermillOperator(
#             task_id = task_id,
#             input_nb = input_nb,
#             output_nb = output_nb,
#             parameters = params,
#             dag = dag,
#             retries = retry_count,
#             retry_delay = timedelta(minutes = retry_delay),
#             **custom_email_callback(email_list, task_log = task_log, attachment_files = [output_nb], on_success = mail_on_success),
#         )
#     else:
#         task = PapermillOperator(
#             task_id = task_id,
#             input_nb = input_nb,
#             parameters = params,
#             dag = dag,
#             retries = retry_count,
#             retry_delay = timedelta(minutes = retry_delay),
#             **custom_email_callback(email_list, task_log = task_log, attachment_files = [output_nb], on_success = mail_on_success),
#         )
#     return task


########################################################
# Sensor and Trigger
########################################################
def create_triggerdag(task_id, dag, params, triggered_dag_id,
                     dag_retry_count = 3, dag_retry_delay = 5, dag_email_list = [], dag_mail_on_success = False, task_log = ''):
    
    retry_count = params.get('retry_count', dag_retry_count)
    retry_delay = params.get('retry_delay', dag_retry_delay)
    email_list = params.get('email_list', dag_email_list)
    mail_on_success = params.get('mail_on_success', dag_mail_on_success)
    task_log = params.get('task_log', task_log)
    
    trigger = TriggerDagRunOperator(
        dag = dag,
        task_id = task_id,
        trigger_dag_id = triggered_dag_id,
        wait_for_completion = True,
        trigger_rule = 'none_failed_or_skipped',
        retries = retry_count,
        retry_delay = timedelta(minutes = retry_delay),
        **custom_email_callback(email_list, task_log = task_log, attachment_files = '', on_success = mail_on_success)
    )
    return trigger


def create_external_task_sensor(task_id, dag, params, external_dag_id, execution_delta = timedelta(days = 1),
                               dag_retry_count = 3, dag_retry_delay = 5, dag_email_list = [], dag_mail_on_success = False, task_log = ''):

    retry_count = params.get('retry_count', dag_retry_count)
    retry_delay = params.get('retry_delay', dag_retry_delay)
    email_list = params.get('email_list', dag_email_list)
    mail_on_success = params.get('mail_on_success', dag_mail_on_success)
    task_log = params.get('task_log', task_log)
    execution_delta = params.get('execution_delta', execution_delta)
    
    sensor = ExternalTaskSensor(
        task_id = task_id,
        external_dag_id = external_dag_id,
        execution_delta = execution_delta,
        timeout = 300,
        trigger_rule = 'none_failed_or_skipped',
        retries = retry_count,
        retry_delay = timedelta(minutes = retry_delay),
        **custom_email_callback(email_list, task_log = task_log, attachment_files = '', on_success = mail_on_success)
    )
    return sensor


def create_sql_sensor():
    pass

########################################################
# Dummy
########################################################
def create_dummy_task(task_id, dag):
    task = DummyOperator(
        dag = dag, 
        task_id = task_id,
        wait_for_downstream = True,
        trigger_rule = 'none_failed_or_skipped'
    )
    return task


def create_bash_task(task_id, dag, params, command,
                    dag_retry_count = 3, dag_retry_delay = 5, dag_email_list = [], dag_mail_on_success = False, task_log = ''):
    
    retry_count = params.get('retry_count', dag_retry_count)
    retry_delay = params.get('retry_delay', dag_retry_delay)
    email_list = params.get('email_list', dag_email_list)
    mail_on_success = params.get('mail_on_success', dag_mail_on_success)
    task_log = params.get('task_log', task_log)
    
    task = BashOperator(
        dag = dag,
        task_id = task_id,
        bash_command = command,
        do_xcom_push = False,
        trigger_rule = 'none_failed_or_skipped',
        retries = retry_count,
        retry_delay = timedelta(minutes = retry_delay),
        **custom_email_callback(email_list, task_log = task_log, attachment_files = '', on_success=mail_on_success),
        )
    return task


def create_START_task(dag):
    task_id = 'START'
    task = create_dummy_task(task_id, dag)
    return task


def create_END_task(dag, dag_name, dag_mail_on_success = True):
    task_id = 'END'
    params = dict()
    command = 'echo 0'
    task_log = f'{dag_name} job run successfully'
    task = create_bash_task(task_id, dag, params, command, 
                            dag_mail_on_success = True, task_log = task_log)
    return task



########################################################
# Python Function
########################################################
from helper.dummy import dummy_task
from helper.database import sql_task
from helper.shell import shell_task
from helper.branch import branch_task
from helper.notebook import notebook_task
function_dict = None
    
def get_function(function_id = None):
    global function_dict
    
    if function_dict == None:
        function_dict = dict()
        
        # Dummy
        function_dict['dummy'] = dummy_task.dummy_task
        function_dict['dummy_task'] = dummy_task.dummy_task
        function_dict['dummy_branch'] = dummy_task.dummy_branch_task
        function_dict['dummy_branch_task'] = dummy_task.dummy_branch_task
        
        
        # SQL
        function_dict['sql'] = sql_task.sql_task
        function_dict['import'] = sql_task.import_db_task
        function_dict['export'] = sql_task.export_db_task
        function_dict['sensor'] = sql_task.sensor_task
        function_dict['import_from_db'] = sql_task.import_from_db_task
        function_dict['validate_records'] = sql_task.validate_records_task
        function_dict['4nalyze_export'] = sql_task.export_to_s3_task
        
        # Notebook
        function_dict['notebook'] = notebook_task.notebook_task
        
        #Shell
        function_dict['shell'] = shell_task.shell_task
        
        
        #Branch
        function_dict['branch'] = branch_task.branch_task
        
        
    if function_id == None:
        return function_dict
    else:
        return function_dict[function_id]

    
def create_task(task_id, row, 
                project_name, project_id, dag_name, env, dag_owner, dag,
                dag_retry_count = 3, dag_retry_delay = 5, dag_email_list = [], dag_mail_on_success = False,
                log_endpoint_id = None
               ):
    job_type = row['job_type']
    params = create_params(row, project_name, project_id, dag_name, env, dag_owner, log_endpoint_id)
    job_type = job_type.lower()
    function_dict = get_function()
    
    
    
    if 'branch' in job_type:
        function_id = job_type
        task = create_branch_task(
            task_id, dag, params, function_id,
            dag_retry_count, dag_retry_delay, dag_email_list, dag_mail_on_success
        )
    
    elif job_type in function_dict.keys():
        function_id = job_type
        task = create_python_task(
            task_id, dag, params, function_id, 
            dag_retry_count, dag_retry_delay, dag_email_list, dag_mail_on_success
        )
    
#     elif job_type in ('notebook'):
#         task = create_notebook_task(
#             task_id, dag, params,
#             dag_retry_count, dag_retry_delay, dag_email_list, dag_mail_on_success
#         )
        
    elif job_type in ('sensor_dag'):
        task = create_external_task_sensor(
            task_id, dag, params, external_dag_id, execution_delta = timedelta(days = 1),
            dag_retry_count=dag_retry_count, dag_retry_delay=dag_retry_delay, dag_email_list=dag_email_list, dag_mail_on_success=dag_mail_on_success
        )
        
    elif job_type in ('trigger_dag'):
        task = create_triggerdag(
            task_id, dag, params, triggered_dag_id,
            dag_retry_count, dag_retry_delay, dag_email_list, dag_mail_on_success
        )
    
    else:
        function_id = row['function_id']
        task = create_python_task(
            task_id, dag, params, function_id, 
            dag_retry_count, dag_retry_delay, dag_email_list, dag_mail_on_success
        )
        
    return task



########################################################
# Parameter
########################################################
def clean_params(params):
    if params is None:
        params = dict()
    else:
        params = params.replace('\\\\','\\\\\\\\')
        params = json.loads(params)
    return params


def create_params(row, project_name, project_id, dag_name, env, dag_owner, log_endpoint_id):
    
    # Parameter
    required_params = clean_params(row['required_params'])
    optional_params = clean_params(row['optional_params'])
    variable_params = clean_params(row['variable_params'])
    
    parameter = dict()
    parameter['LOG_ENDPOINT_ID'] = log_endpoint_id # Log Endpoint Parameter (Can replace in task parameter)
    if row['job_type'] == 'branch':
        parameter.update({'KEY_DICT':required_params})
    else:
        parameter.update(required_params)
    parameter.update(optional_params)
    parameter.update(variable_params)
    
    # Dag Parameter
    parameter['ENV'] = env
    parameter['UPDATE_BY'] = dag_owner
    
    # General Parameter
    parameter['PROJECT_NAME'] = project_name
    parameter['PROJECT_ID'] = project_id
    parameter['DAG_NAME'] = dag_name
    parameter['ZONE_NAME'] = row['zone_name']
    parameter['SUB_ZONE_NAME'] = row['sub_zone_name']
    parameter['JOB_NAME'] = row['job_name']
    parameter['JOB_TYPE'] = row['job_type']
    parameter['DETAIL'] = row['detail']
    parameter['ASAT_DT_FLAG'] = row['ASAT_DT_FLAG']
    parameter['AIRFLOW_ROOT'] = root_path
    
    # Remove None Value
    parameter = {k: v for k, v in parameter.items() if v is not None}
        
    params = {
        'parameter' : parameter
    }
    
    task_params = ['retry_count', 'retry_delay', 'email_list', 'mail_on_success']
    for task_param in task_params:
        if task_param in row.keys():
            value = row[task_param]
            if value is not None and str(row[task_param]).lower() != 'nan':
                if task_param in ['retry_count', 'retry_delay']:
                    params[task_param] = int(row[task_param])
                elif task_param in ['email_list']:
                    params[task_param] = [email.strip() for email in row[task_param].split(',')]
                else:
                    params[task_param] = row[task_param]
            
    return params



    














