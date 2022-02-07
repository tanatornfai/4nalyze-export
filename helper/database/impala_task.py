import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

import re

from helper.file import file_manager
from helper.file.hdfs_helper import hdfs_helper
from helper.database.impala_helper import impala_helper
from helper.utility.param_helper import replace_params_intext, read_parameter


##########################################################################
# Import and Export
##########################################################################
def impala_import_task(parameter, **context):
    impala_import_job(parameter, context)
    
def impala_import_job(parameter, context):
    
    # Required parameter
    endpoint_type = parameter['ENDPOINT_TYPE'].upper().strip()
    endpoint_id   = parameter['ENDPOINT_ID']
    database      = parameter['DATABASE']
    table         = parameter['TABLE']

    # Optional parameter
    local_tmp_dir = parameter.get('LOCAL_TMP_DIR', '${AIRFLOW_ROOT}/tmp/${DATABASE}/${TABLE}/')
    local_tmp_dir = replace_params_intext(local_tmp_dir, parameter)
    if endpoint_type == 'IMPALA':
        overwrite         = parameter.get('OVERWRITE', True)
        partition_columns = parameter.get('PARTITION_COLUMNS', '').split(',')
        partition_columns = [col.strip() for col in partition_columns]
        partition_columns = [col for col in partition_columns if len(col) > 0]
        partition_values  = parameter.get('PARTITION_VALUES', '').split(',')
        partition_values = [val.strip() for val in partition_values]
        partition_values = [val for val in partition_values if len(val) > 0]
        
        if len(partition_columns) != len(partition_values):
            raise Exception(f'Length of Partition columns and Partition values is not equal {partition_columns} | {partition_values}')
        if len(partition_columns) == 0:
            partition_list = None
        else:
            partition_list    = [(col,val) for col,val in zip(partition_columns, partition_values)]
        
    elif endpoint_type == 'KUDU':
        pass
    
    else:
        raise Exception(f'Endpoint Type not unknown : {endpoint_type}')
    show_output = parameter.get('SHOW_OUTPUT', True)
    
    
    
    print('=========================================')
    print('Create Connection')
    print('=========================================')
    hdfs = hdfs_helper(endpoint_id)
    impala = impala_helper(endpoint_id, hdfs)
    
    
    
    print('=========================================')
    print('Import ')
    print('=========================================')
    if endpoint_type == 'IMPALA':
        impala.import_impala(
            database, table, 
            local_tmp_dir = local_tmp_dir, 
            process_date = parameter['PROCESS_DATE'],
            overwrite = overwrite, partition_list = partition_list
        )
    else:
        impala.import_kudu(
            database, table, 
            local_tmp_dir = local_tmp_dir, 
            process_date = parameter['PROCESS_DATE']
        )
        
        
        
    print('=========================================')
    print('Insert Log')
    print('=========================================')
    # Insert Log to ???
    # xxx
    
    
    
##########################################################################
# Execute and Sensor
##########################################################################    
def impala_execute_task(parameter, **context):
    impala_execute_job(parameter, context)
        
def impala_execute_job(parameter, context):
    
    # Required parameter
    endpoint_id = parameter['ENDPOINT_ID']
    sql_type = parameter['SQL_TYPE'].upper().strip()
    if sql_type == 'SQL_PATH':
        sql_path = parameter['SQL_PATH']
    elif sql_type == 'SQL_SCRIPT':
        sql_script = parameter['SQL_SCRIPT']

    # Optional parameter
    show_output = parameter.get('SHOW_OUTPUT', True)
        
        
        
    print('=========================================')
    print('Create Connection')
    print('=========================================')
    hdfs = hdfs_helper(endpoint_id)
    impala = impala_helper(endpoint_id, hdfs)
    
    
    
    if sql_type == 'SQL_PATH':
        print('=========================================')
        print('Get SQL Script')
        print('=========================================')
        print(f'Getting SQL Script from {sql_path}')
        sql_script = file_manager.read_file(sql_path)
        print(f'RAW SQL SCRIPT : \n{sql_script}\n\n')

        print('Replacing Variable in Script')
        sql_script = replace_params_intext(sql_script, parameter)
    print(f'SQL SCRIPT : \n{sql_script}\n\n')

        
        
    print('=========================================')
    print('Run SQL Script')
    print('=========================================')
    sql_script_list = [script.strip('\n').strip() for script in re.split('; |;\n|;\t', sql_script)]
    results_list = []
    for script in sql_script_list:
        print(f'Running Script : \n{script}')
        results = impala.execute_df(script)
        results_list.append(results)
        if show_output:
            print(f'SQL Result : \n{results}')
        print('----------------------------------------------------\n\n\n')
    
    
    
    print('=========================================')
    print('Insert Log')
    print('=========================================')
    # Insert Log to ???
    # xxx
    
    return results_list
    
    
    
def impala_sensor_task(parameter, **context):
    impala_sensor_job(parameter, context)
        
def impala_sensor_job(parameter, context):
    
    # Required parameter
    endpoint_id = parameter['ENDPOINT_ID']
    sql_type = parameter['SQL_TYPE'].upper().strip()
    if sql_type == 'SQL_PATH':
        sql_path = parameter['SQL_PATH']
    elif sql_type == 'SQL_SCRIPT':
        sql_script = parameter['SQL_SCRIPT']

    # Optional parameter
    show_output = parameter.get('SHOW_OUTPUT', True)
    row_count = int(parameter.get('ROW_COUNT', 1))
    compare_type = parameter.get('COMPARE_TYPE', 'MIN').upper() # EXACT, MIN
        
        
        
    print('=========================================')
    print('Create Connection')
    print('=========================================')
    hdfs = hdfs_helper(endpoint_id)
    impala = impala_helper(endpoint_id, hdfs)
    
    
    
    if sql_type == 'SQL_PATH':
        print('=========================================')
        print('Get SQL Script')
        print('=========================================')
        print(f'Getting SQL Script from {sql_path}')
        sql_script = file_manager.read_file(sql_path)
        print(f'RAW SQL SCRIPT : \n{sql_script}\n\n')

        print('Replacing Variable in Script')
        sql_script = replace_params_intext(sql_script, parameter)
    print(f'SQL SCRIPT : \n{sql_script}\n\n')

        
        
    print('=========================================')
    print('Run SQL Script')
    print('=========================================')
    df = impala.execute_df(sql_script)
    if show_output:
        print(f'len(df) : {len(df)}')
        print(f'SQL Result : \n{df}')
        
        
        
    this_row_count = len(df)
    if compare_type == 'MIN':
        if this_row_count >= row_count:
            pass
        else:
            raise Exception(f'return row count ({this_row_count}) not greater than expected row count ({row_count})')
    elif compare_type == 'EXACT':
        if this_row_count == row_count:
            pass
        else:
            raise Exception(f'return row count ({this_row_count}) not equal expected row count ({row_count})')
    else:
        raise Exception(f'Unknown Compare_type : {compare_type}')