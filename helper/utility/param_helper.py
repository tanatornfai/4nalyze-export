import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

from helper.utility.time_helper import get_multi_datetime_jinja, get_multi_datetime_context



######################################
# Read Parameter
######################################
def replace_params_intext(text, params_dict):
    if isinstance(text, str):
        for key in params_dict.keys():
            text = text.replace('${%s}'%key, str(params_dict[key]))
    return text


def read_parameter(parameter, context = None):
    print('=========================================')
    print('PARAMETERS')
    print('=========================================')
    print('parameter :')
    for key in parameter.keys():
        print(f'\t {key} : {parameter[key]}')
    
    # GET DATE
    asat_dt_flag = parameter.get('ASAT_DT_FLAG', 'PROCESS_DATE')
    if context is None:
        asat_dt, process_date, execution_date, first_date, last_date, now = get_multi_datetime_jinja(parameter, asat_dt_flag)
    else:
        asat_dt, process_date, execution_date, first_date, last_date, now = get_multi_datetime_context(context, asat_dt_flag)
    parameter['ASAT_DT'] = asat_dt
    parameter['PROCESS_DATE'] = str(process_date)
    parameter['EXECUTION_DATE'] = str(execution_date)
    parameter['FIRST_DATE_OF_MONTH'] = str(first_date)
    parameter['LAST_DATE_OF_PREV_MONTH'] = str(last_date)
    parameter['NOW'] = now
    

    i = 0
    while True:
        for key in parameter.keys():
            parameter[key] = replace_params_intext(parameter[key], parameter)
        
        incomplete_parameters = [value for value in parameter.values() if isinstance(value, str) and '${' in value and '}' in value]
        if len(incomplete_parameters) == 0:
            break
        if i > 100:
            for key in parameter.keys():
                print(f'\t {key} : {repr(parameter[key])}')
            raise Exception('Parameter Loop')
        else:
            i += 1
            
    print('\n')
    print('=========================================')
    print('PARAMETERS (Post Processed)')
    print('=========================================')
    for key in parameter.keys():
        print(f'\t {key} : {repr(parameter[key])}')

    return parameter



