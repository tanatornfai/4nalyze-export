import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

import subprocess



class shell_helper:
    
    def __init__(self):
        pass

    
    
    ##########################################
    # Execute script
    ##########################################
    def send_command(self, command, check_returncode = True):        
        print('Executing Script on Local')
        try:
            stdout = subprocess.check_output(
                command, 
                stderr = subprocess.STDOUT, 
                shell = True, 
                timeout = 300,
                universal_newlines = True
            )
            
        except subprocess.CalledProcessError as exc:
            return_code = exc.returncode
            stdout = ''
            stderr = exc.output
            
            print(f'Shell script error detected !!!')
            if check_returncode:
                raise Exception(f'Run Shell Script Error - return code : {return_code}')
                
        else:
            return_code = 0
            stderr = ''
            
            print('\tRun Script Successfully\n\n')
            
        return return_code, stdout, stderr