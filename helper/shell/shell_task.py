import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

from helper.shell.ssh_helper import ssh_helper
from helper.shell.shell_helper import shell_helper
from helper.utility.param_helper import read_parameter



def shell_task(parameter, **context):
    shell_job(parameter, context)
        
def shell_job(parameter, context):
    parameter = read_parameter(parameter, context)
    
    # Required parameter
    endpoint_id = parameter['ENDPOINT_ID']
    shell_type = parameter['SHELL_TYPE'].upper().strip()
    if shell_type == 'SHELL_PATH':
        shell_path = parameter['SHELL_PATH']
    elif shell_type == 'SHELL_SCRIPT':
        shell_script = parameter['SHELL_SCRIPT']

    # Optional parameter
    check_returncode = parameter.get('CHECK_RETURNCODE', True)
    show_output = parameter.get('SHOW_OUTPUT', True)
    push_xcom = parameter.get('PUSH_XCOM', False)
        
        
    if endpoint_id.upper() == 'LOCALHOST':
        ssh = shell_helper()
    else:
        print('=========================================')
        print('Create Connection')
        print('=========================================')
        ssh = ssh_helper(endpoint_id)
    
    
    
    if shell_type == 'SHELL_PATH':
        print('=========================================')
        print('Get SHELL Script')
        print('=========================================')
        print(f'Getting Shell Script from {shell_path}')
        shell_script = file_manager.read_file(shell_path)
        print(f'RAW SHELL SCRIPT : \n{shell_script}\n\n')

        print('Replacing Variable in Script')
        shell_script = replace_params_intext(shell_script, parameter)
    print(f'SHELL SCRIPT : \n{shell_script}\n\n')

        
        
    print('=========================================')
    print('Run SHELL Script')
    print('=========================================')
    return_code, stdout, stderr = ssh.send_command(shell_script, check_returncode)
    if show_output:
        print(f'RETURN CODE : {return_code}\n')
        print(f'STDOUT : \n{stdout}\n')
        print(f'STDERR :\n{stderr}\n')
    if push_xcom:
        key = "return_code"
        value = return_code
        print(f"XCOM Pushed | key : {key} | Value : {value} ")
        context['ti'].xcom_push(key=key, value=value)
    
    
    print('=========================================')
    print('Insert Log')
    print('=========================================')
    # Insert Log to ???
    # xxx