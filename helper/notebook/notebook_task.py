import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

from datetime import datetime
from helper.file import file_manager
from helper.shell.ssh_helper import ssh_helper
from helper.shell.shell_helper import shell_helper
from helper.utility.param_helper import read_parameter, replace_params_intext
from helper.utility.time_helper import get_process_date
from helper.utility.log_helper import log_helper 


#=================================================
# Notebook Execution task
#=================================================
def notebook_task(parameter, **context):
    run_notebook_job(parameter, context)
def run_notebook_job(parameter, context):
    
    parameter = read_parameter(parameter, context)
    # DAG Parameter
    log_endpoint_id = parameter.get('LOG_ENDPOINT_ID', None) 
    
    start = datetime.now()
    try:
        # Correct parameters
        notebook_name = parameter['NOTEBOOK_NAME'].replace('.ipynb','')
        notebook_name = replace_params_intext(notebook_name, parameter)

        endpoint_id = parameter['ENDPOINT_ID']
        input_dir = parameter['NOTEBOOK_DIR']
        input_dir = replace_params_intext(input_dir, parameter)
        save_log = parameter.get('SAVE_LOG', True)
        push_xcom = parameter.get('PUSH_XCOM', False)

        project_id = parameter['PROJECT_ID']
        dag_name = parameter['DAG_NAME']
        zone_name = parameter['ZONE_NAME']
        job_name = parameter['JOB_NAME']

        airflow_root = parameter['AIRFLOW_ROOT']
        output_dir = parameter.get('OUTPUT_DIR', f'/usr/local/airflow/log/{project_id}/{dag_name}/{zone_name}/{job_name}/')

        execution_date = context['execution_date']
        #Create Endpoint
        if endpoint_id.upper() == 'LOCALHOST':
            ssh = shell_helper()
        else:
            ssh = ssh_helper(endpoint_id)
        print('=================================================')
        print('Checking Directory availability')
        #Check Directory availability
        linux_list_cmd,window_list_cmd = create_dir_if_not_exist(endpoint_id,output_dir)
        run_success = False
        try:
            print("Try check directory via linux script")
            ssh.send_command(linux_list_cmd)
            run_success = True
        except:
            print("Try check directory via window script")
            ssh.send_command(window_list_cmd)
            run_success = True
        if run_success == False:
            raise Exception(f"Can't Create folder {output_dir} on {endpoint_id}")
        print('=================================================')
        print(f'Executing Notebook at Endpoint : {endpoint_id}')
        #Executing Notebook
        notebook_params = get_notebook_params(parameter)
        output_nb,cmd = create_papermill_command(endpoint_id,input_dir,output_dir,notebook_name,notebook_params,save_log,execution_date)
        return_code, stdout, stderr = ssh.send_command(cmd)
        print('=================================================')
        print()

        #Save Output
        if save_log:
            clear_destination = False
            save_path = f"{airflow_root}/log/{project_id}/{dag_name}/{zone_name}/{job_name}/{notebook_name}_{execution_date}.ipynb"
            source_path = create_source_path(endpoint_id,output_nb)
            file_manager.download_file(source_path,save_path,clear_destination)
        if push_xcom:
            key = "return_code"
            value = return_code
            print(f"XCOM Pushed | key : {key} | Value : {value} ")
            context['ti'].xcom_push(key=key, value=value)
            
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
        

#=================================================
# Notebook Utility
#=================================================
def create_source_path(endpoint_id,output_nb):
    if endpoint_id.upper() != 'LOCALHOST':
        endpoint_type = 'REMOTE'
        result = f'{endpoint_type}://{endpoint_id}/{output_nb}'
    else:
        result = output_nb
    return result
def create_dir_if_not_exist(endpoint_id,dir_name):
    print(f"Preparing Directory : {dir_name}")
    linux_list_cmd = f"""if [ -d "{dir_name}" ] ; then echo "Directory {dir_name} exists."; else mkdir -p {dir_name}; fi"""
    window_list_cmd = f"""IF exist "{dir_name}" ( echo "{dir_name}" exists ) ELSE ( mkdir "{dir_name}" && echo {dir_name} created)"""
    return linux_list_cmd,window_list_cmd

def get_notebook_params(params):
    tmp = ''
    for param in params:
        tmp = tmp + f"-p {param} '{params[param]}' "
    return tmp
def create_papermill_command(endpoint_id,input_dir,output_dir,notebook_name,notebook_params,save_log,execution_date):
    if input_dir[-1] != '/':
        input_dir += '/'
    input_nb = f'{input_dir}{notebook_name}.ipynb'
    output_nb = f"{output_dir}/{notebook_name}_{execution_date}.ipynb"
    cmd = f'papermill {input_nb} {output_nb} {notebook_params}'
    return output_nb,cmd