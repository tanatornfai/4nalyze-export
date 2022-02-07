import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

import pandas as pd
import json
import time
import numpy as np
from helper.file import file_manager
from helper.database.rs_helper import rs_helper
from helper.utility.param_helper import replace_params_intext, read_parameter


#=================================================
# Importing task
#=================================================

def rs_import_job(parameter, context):

    endpoint_id = parameter['ENDPOINT_ID']
    target = parameter['TABLE']
    file_path = parameter['FILE_PATH']
    iamrole = parameter['IAMROLE']
    additional_arg = parameter.get('ADDITIONAL_ARGUMENTS', '')
    overwrite = parameter.get('OVERWRITE',True)
    push_xcom = parameter.get('PUSH_XCOM', False)
    #Create Endpoint
    rs = rs_helper(endpoint_id)
    database = parameter.get('DATABASE',None)
    if database == None:
        database = rs.database
    print('=================================================')
    print('Checking file availability')
    #Check bucket availability
    file_list = file_manager.list_file(file_path,show_log=True)
    len_file_list = len(file_list)
    if len_file_list == 0:
        raise Exception(f'No file matching FILE_PATH pattern : {file_path}')
    else :
        print(f'Having {len_file_list} files in path {file_path}')
    print('Pass files checking')
    print('=================================================')
    if overwrite:
        print('Overwrite set to True')
        rs.truncate(target,database)
        print('=================================================')
    bucket = file_path.split('//')[-1].split('/')[0]
    for idx,file in enumerate(file_list):
        print(f'Inserting file {idx+1}/{len_file_list} : {file}')
        df = rs.load_data(target,f's3://{bucket}/{file}',iamrole,database,additional_arg)
    print('=================================================')
    if push_xcom:
        if df.count()[0] == 0:
            raise Exception('No data returned')
        elif df.count()[0] > 1:
            raise Exception('Multiple rows returned')
        json_df = json.loads(df.to_json(orient='columns'))
        for key in json_df:
            value = json_df[key][str(df.index[0])]
            if value == '' or value == None:
                continue
            print(f"XCOM Pushed | key : {key} | Value : {value} ")
            context['ti'].xcom_push(key=key, value=value)
            
            
#=================================================
# Executing task
#=================================================


def rs_execute_job(parameter, context):

    endpoint_id = parameter['ENDPOINT_ID']
    sql_type = parameter['SQL_TYPE']
    if sql_type == 'SQL_PATH':
        sql_path = parameter['SQL_PATH']
        sql_script = file_manager.read_file(sql_path)
    elif sql_type == 'SQL_SCRIPT':
        sql_script = parameter['SQL_SCRIPT']
    
    print('=================================================')
    print('Using Base SQL :')
    print(sql_script)
    print('=================================================')
    print('Replacing Parameters in Base SQL :')
    sql_script = replace_params_intext(sql_script,parameter)
    print('Using SQL :')
    print(sql_script)
    print('=================================================')
    show_output = parameter.get('SHOW_OUTPUT', False)
    push_xcom = parameter.get('PUSH_XCOM', False)
    
    #Create Endpoint
    rs = rs_helper(endpoint_id)
    database = parameter.get('DATABASE',None)
    if database == None:
        database = rs.database
    print('=================================================')
    if show_output:
        df = rs.execute_pandas(sql_script,database)
        print(df.head())
        if push_xcom:
            if df.count()[0] == 0:
                raise Exception('No data returned')
            elif df.count()[0] > 1:
                raise Exception('Multiple rows returned')
            json_df = json.loads(df.to_json(orient='columns'))
            for key in json_df:
                value = json_df[key][str(df.index[0])]
                if value == '' or value == None:
                    continue
                print(f"XCOM Pushed | key : {key} | Value : {value} ")
                context['ti'].xcom_push(key=key, value=value)
    else:
        rs.execute(sql_script,database)

#=================================================
# Exporting task
#=================================================

def rs_export_job(parameter, context):

    endpoint_id = parameter['ENDPOINT_ID']
    sql_type = parameter['SQL_TYPE']
    if sql_type == 'SQL_PATH':
        sql_path = parameter['SQL_PATH']
        sql_script = file_manager.read_file(sql_path)
    elif sql_type == 'SQL_SCRIPT':
        sql_script = parameter['SQL_SCRIPT']
    
    print('=================================================')
    print('Using Base SQL :')
    print(sql_script)
    print('=================================================')
    print('Replacing Parameters in Base SQL :')
    sql_script = replace_params_intext(sql_script,parameter)
    print('Using SQL :')
    print(sql_script)
    print('=================================================')
    destination = parameter['DESTINATION_PATH']
    iamrole = parameter['IAMROLE']
    additional_arg = parameter.get('ADDITIONAL_ARGUMENTS', '')
    clear_dir = parameter.get('CLEAR_OUTPUT_PATH', True)
    push_xcom = parameter.get('PUSH_XCOM', False)
    #Create Endpoint
    rs = rs_helper(endpoint_id)
    database = parameter.get('DATABASE',None)
    if database == None:
        database = rs.database
    print('=================================================')
    print('Checking Destination Path')
    #Check bucket availability
    original_list = file_manager.list_file(destination,show_log=False)
    print(f"Clear Output path : {clear_dir}")
    if clear_dir:
        file_manager.delete_file(destination)
    print('Pass Files checking')
    print('=================================================')
    
    df = rs.export_data(sql_script,destination,iamrole,database,additional_arg=additional_arg)
    print('=================================================')
    print('Checking output files')
    output_list = file_manager.list_file(destination,show_log=True)
    
    print(f"Output have {len(output_list)} files in path {destination}")
    if push_xcom:
        if df.count()[0] == 0:
            raise Exception('No data returned')
        elif df.count()[0] > 1:
            raise Exception('Multiple rows returned')
        json_df = json.loads(df.to_json(orient='columns'))
        for key in json_df:
            value = json_df[key][str(df.index[0])]
            if value == '' or value == None:
                continue
            print(f"XCOM Pushed | key : {key} | Value : {value} ")
            context['ti'].xcom_push(key=key, value=value)
    
def rs_sensor_job(parameter, context):
    
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

    print('=================================================')
    print('Using Base SQL :')
    print(sql_script)
    print('=================================================')
    print('Replacing Parameters in Base SQL :')
    sql_script = replace_params_intext(sql_script,parameter)
    print('Using SQL :')
    print(sql_script)
    print('=================================================')
    show_output = parameter.get('SHOW_OUTPUT', False)
    
    #Create Endpoint
    rs = rs_helper(endpoint_id)
    database = parameter.get('DATABASE', rs.database)
    df = rs.execute_pandas(sql_script, database)

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