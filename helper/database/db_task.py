import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

import re
import math
import multiprocessing as mp
from datetime import datetime
from helper.file import file_manager
from helper.database.mssql_helper import mssql_helper
from helper.database.mysql_helper import mysql_helper
from helper.database.postgresql_helper import postgresql_helper
from helper.utility.param_helper import read_parameter,replace_params_intext
from helper.utility.log_helper import log_helper 
from helper.database.db_helper import select_db_helper
        
def db_import_job(parameter, context):
    
    # Required parameter
    endpoint_id = parameter['ENDPOINT_ID']
    table = parameter['TABLE']
    input_path = parameter['INPUT_PATH']

    # Optional parameter
    overwrite = parameter.get('OVERWRITE', True)
    optional_arguments = parameter.get('OPTIONAL_ARGUMENTS', dict())
    host_endpoint_id = parameter.get('HOST_ENDPOINT_ID', None)
    push_xcom = parameter.get('PUSH_XCOM', False)
        
        
    print('=========================================')
    print('Create Connection')
    print('=========================================')
    db = select_db_helper(endpoint_id, host_endpoint_id)
    
    if overwrite:
        print('=================================================')
        print('Overwrite set to True')
        print('=================================================')
        db.truncate(table)
        
    print('=========================================')
    print('Run Import Data')
    print('=========================================')
    if host_endpoint_id is not None:
        return_code, stdout, stderr = db.import_data(table, input_path, optional_arguments)
        if push_xcom:
            key = "return_code"
            value = return_code
            print(f"XCOM Pushed | key : {key} | Value : {value} ")
            context['ti'].xcom_push(key=key, value=value)
    else:
        db.import_data(table, input_path, optional_arguments)


def db_execute_job(parameter, context):
    
    # Required parameter
    endpoint_id = parameter['ENDPOINT_ID']
    sql_type = parameter['SQL_TYPE'].upper().strip()
    if sql_type == 'SQL_PATH':
        sql_path = parameter['SQL_PATH']
    elif sql_type == 'SQL_SCRIPT':
        sql_script = parameter['SQL_SCRIPT']

    # Optional parameter
    show_output = parameter.get('SHOW_OUTPUT', False)
    push_xcom = parameter.get('PUSH_XCOM', False)
                     
    print('=========================================')
    print('Create Connection')
    print('=========================================')
    db = select_db_helper(endpoint_id)
        
    print('=========================================')
    print('SQL Script')
    print('=========================================')
    if sql_type == 'SQL_PATH':
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
        if show_output:
            results = db.execute_df(sql_script)
            print(f'SQL Result : \n{results.to_string()}')
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
            db.execute(sql_script)
        print('----------------------------------------------------\n\n\n')


def db_export_job(parameter, context):
    
    # Required parameter
    endpoint_id = parameter['ENDPOINT_ID']
    sql_type = parameter['SQL_TYPE']
    if sql_type == 'SQL_PATH':
        sql_path = parameter['SQL_PATH']
        sql_script = file_manager.read_file(sql_path)
    elif sql_type == 'SQL_SCRIPT':
        sql_script = parameter['SQL_SCRIPT']
    output_path = parameter['OUTPUT_PATH']

    # Optional parameter
    optional_arguments = parameter.get('OPTIONAL_ARGUMENTS', dict())
    host_endpoint_id = parameter.get('HOST_ENDPOINT_ID', None)
    push_xcom = parameter.get('PUSH_XCOM', False)
    
        
    print('=========================================')
    print('Create Connection')
    print('=========================================')
    db = select_db_helper(endpoint_id, host_endpoint_id)
    
    
    print('=========================================')
    print('SQL Script')
    print('=========================================')
    if sql_type == 'SQL_PATH':
        print(f'Getting SQL Script from {sql_path}')
        sql_script = file_manager.read_file(sql_path)
        print(f'RAW SQL SCRIPT : \n{sql_script}\n\n')

        print('Replacing Variable in Script')
        sql_script = replace_params_intext(sql_script, parameter)
    print(f'SQL SCRIPT : \n{sql_script}\n\n')
    
    
    print('=========================================')
    print('Run Export Data')
    print('=========================================')
    if host_endpoint_id is not None:
        return_code, stdout, stderr = db.export_data(sql_script, output_path, optional_arguments)
        if push_xcom:
            key = "return_code"
            value = return_code
            print(f"XCOM Pushed | key : {key} | Value : {value} ")
            context['ti'].xcom_push(key=key, value=value)
    else:
        db.export_data(sql_script, output_path, optional_arguments)

    
def db_sensor_job(parameter, context):
    
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
    db = select_db_helper(endpoint_id)
    
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
    df = db.execute_df(sql_script)
    
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
        
def import_from_db_batch(batch_parameters):
    try:
        # parameters
        batch_id = batch_parameters["batch_id"]
        etl_host_endpoint_id = batch_parameters["etl_host_endpoint_id"]
        tmp_file = batch_parameters["tmp_file"]
        source_endpoint_id = batch_parameters["source_endpoint_id"]
        source_sql_script = batch_parameters["source_sql_script"]
        start_row = batch_parameters["start_row"]
        end_row = batch_parameters["end_row"]
        source_optional_arguments = batch_parameters["source_optional_arguments"]
        destination_endpoint_id = batch_parameters["destination_endpoint_id"]
        destination_table = batch_parameters["destination_table"]
        destination_optional_arguments = batch_parameters["destination_optional_arguments"]
        print_log = batch_parameters.get("print_log", False)
        return_log = batch_parameters.get("return_log", True)
        
        if return_log is True:
            log = f'Batch: {batch_id}, ETL Records: {start_row} - {end_row}, Temp File Path: {tmp_file}\n'

        source_db = select_db_helper(source_endpoint_id, etl_host_endpoint_id, print_log=print_log)
        result1 = source_db.export_data(source_sql_script, tmp_file, source_optional_arguments, 
                                                    print_log=print_log, return_log=return_log,
                                                    start_row=start_row, end_row=end_row)
        if return_log is True:
            _, _, _, export_log = result1
            log += export_log

        destination_db = select_db_helper(destination_endpoint_id, etl_host_endpoint_id, print_log=print_log)
        result2 = destination_db.import_data(destination_table, tmp_file, destination_optional_arguments, 
                                                         print_log=print_log, return_log=return_log)
        if return_log is True:
            _, _, _, import_log = result2
            log += import_log
            return log
    except Exception as err:
        return err
        
def db_import_from_db_job(parameter, context):
    
    # Required parameter
    temp_dir = parameter['TEMP_DIR']
    etl_host_endpoint_id = parameter['ETL_HOST_ENDPOINT_ID']
    source_endpoint_id = parameter['SOURCE_ENDPOINT_ID']
    destination_endpoint_id = parameter['DESTINATION_ENDPOINT_ID']
    destination_table = parameter['DESTINATION_TABLE']
    source_columns = parameter['SOURCE_COLUMNS']
    source_table = parameter['SOURCE_TABLE'] 
    source_sql_script = f'select {source_columns} from {source_table}'
     
    # Optional parameter
    batch_size = parameter.get('BATCH_SIZE', None)
    batch_size = int(batch_size) if (batch_size is not None) else None
    parallel = int(parameter.get('PARALLEL', 1))
    limit_row = parameter.get('LIMIT_ROW', None)
    overwrite = parameter.get('OVERWRITE', True)
    source_optional_arguments = parameter.get('SOURCE_OPTIONAL_ARGUMENTS', dict())
    destination_optional_arguments = parameter.get('DESTINATION_OPTIONAL_ARGUMENTS', dict())
    pre_clear_dir = parameter.get('PRE_CLEAR_DIR', False)
    post_clear_dir = parameter.get('POST_CLEAR_DIR', False)
    source_condition = parameter.get('SOURCE_CONDITION', None)
    source_order_by = parameter.get('SOURCE_ORDER_BY', None)
    if source_condition is not None:
        source_sql_script = f'{source_sql_script} {source_condition}'
    if source_order_by is not None:
        source_sql_script = f'{source_sql_script} order by {source_order_by}'
        
    print('=========================================')
    print('Test Source Connection')
    print('=========================================')
    source_db = select_db_helper(source_endpoint_id, etl_host_endpoint_id)
    
    print('=========================================')
    print('Test Destination Connection')
    print('=========================================')
    destination_db = select_db_helper(destination_endpoint_id, etl_host_endpoint_id)
    
    print('=========================================')
    print('SQL Export Script')
    print('=========================================')
    print(f'SQL SCRIPT : \n{source_sql_script}\n\n')
    
    print('=========================================')
    print('Count Source Records')
    print('=========================================')
    count_sql = source_db.generate_count_row_sql(source_table, source_condition)
    print(f'COUNT SQL: {count_sql}')
    df = source_db.execute_df(count_sql)
    num_rows = int(df[df.columns[0]][0])
    if limit_row is not None:
        num_rows = min(int(limit_row), num_rows)
    print(f'TOTAL ROWS: {num_rows}\n\n')
    
    if overwrite:
        print('=================================================')
        print('Overwrite Destination Table set to True')
        print('=================================================')
        destination_db.truncate(destination_table)
    
    print('=================================================')
    print(f'Checking file in {temp_dir}')
    print('=================================================')
    file_path =  f'REMOTE://{etl_host_endpoint_id}/{temp_dir}'
    check_directory = file_manager.check_dir(file_path)
    
    if pre_clear_dir:
        print('=================================================')
        print('Pre Clear Temp Directory')
        print('=================================================')
        file_list = file_manager.list_file(file_path)
        print(f'Delete files in {temp_dir}.')
        delete_file = file_manager.delete_file(file_path)
        print('=================================================\n')
    
    if batch_size is not None and batch_size > 0:
        num_batch = math.ceil(num_rows/batch_size)
        print('=========================================')
        print('Run ETL Data')
        print(f'Parallel: {parallel}')
        print(f'Batch Size: {batch_size}')
        print(f'Number of Batch: {num_batch}')
        print('=========================================\n')
        
        all_batch_parameters = list()
        for i in range(num_batch):
            start_row = i*batch_size + 1
            end_row = min((i+1)*batch_size, num_rows)
            tmp_file = temp_dir.rstrip('/') + f'/tmp_{i+1}'
            batch_parameters = {
                "batch_id": i+1,
                "etl_host_endpoint_id": etl_host_endpoint_id,
                "tmp_file": tmp_file,
                "source_endpoint_id": source_endpoint_id,
                "source_sql_script": source_sql_script,
                "start_row": start_row,
                "end_row": end_row,
                "source_optional_arguments": source_optional_arguments,
                "destination_endpoint_id": destination_endpoint_id,
                "destination_table": destination_table,
                "destination_optional_arguments": destination_optional_arguments
            }
            all_batch_parameters.append(batch_parameters)
            #### For Debug Parallel
            if parallel == 1:
                batch_parameters['print_log'] = True
                batch_parameters['return_log'] = False
                result = import_from_db_batch(batch_parameters)
                if isinstance(result, Exception):
                    raise result
        
        if parallel > 1:
            pool = mp.Pool(processes=parallel)
            for result in pool.imap(import_from_db_batch, all_batch_parameters):
                if isinstance(result, Exception):
                    raise result
                log = result
                print(log)
            pool.close()
            pool.join()
    else:
        print('=========================================')
        print('Run ETL Data')
        print('=========================================\n')
        
        tmp_file = temp_dir.rstrip('/') + f'/tmp'
        source_db.export_data(source_sql_script, tmp_file, source_optional_arguments)
        destination_db.import_data(destination_table, tmp_file, destination_optional_arguments)

    if post_clear_dir:
        print('=================================================')
        print('Post Clear Temp Directory')
        print('=================================================')
        file_list = file_manager.list_file(file_path)
        print(f'Delete files in {temp_dir}.')
        delete_file = file_manager.delete_file(file_path)
        print('=================================================\n')
            
        
def db_validate_records_job(parameter, context):
    
    # Required parameter
    etl_host_endpoint_id = parameter.get('ETL_HOST_ENDPOINT_ID', None)
    source = parameter['SOURCE']
    source_endpoint_id = parameter.get('SOURCE_ENDPOINT_ID', None)
    destination = parameter['DESTINATION']
    destination_endpoint_id = parameter.get('DESTINATION_ENDPOINT_ID', None)
    source_type = parameter['SOURCE_TYPE']
    destination_type = parameter['DESTINATION_TYPE']
    
    source_records = None
    print('=========================================')
    print('Source Records')
    print('=========================================')    
    if source_type == 'FILE':
        file_path =  f'REMOTE://{etl_host_endpoint_id}/{source}'
        source_records = file_manager.count_rows(file_path)
        print(f'TOTAL ROWS: {source_records}')
    elif source_type == 'DB':
        source_db = select_db_helper(source_endpoint_id)
        print('SQL script:', source)
        df = source_db.execute_df(source)
        source_records = int(df[df.columns[0]][0])
        print(f'TOTAL ROWS: {source_records}')
    
    destination_records = None
    print('=========================================')
    print('Destination Records')
    print('=========================================')    
    if destination_type == 'FILE':
        file_path =  f'REMOTE://{etl_host_endpoint_id}/{destination}'
        destination_records = file_manager.count_rows(file_path)
        print(f'TOTAL ROWS: {source_records}')
    elif destination_type == 'DB':
        destination_db = select_db_helper(destination_endpoint_id)
        print('SQL script:', destination)
        df = destination_db.execute_df(destination)
        destination_records = int(df[df.columns[0]][0])
        print(f'TOTAL ROWS: {destination_records}')
        
    if int(source_records) != int(destination_records):
        raise Exception('Validate records failed - Source records NOT EQUAL to Destination records.')
    else:
        print('Validate records successfully - Source records EQUAL to Destination records.')

def export_to_s3_job(parameter,context):
    parameter = read_parameter(parameter, context)
    # Required parameter
    temp_dir = parameter['TEMP_DIR']
    etl_host_endpoint_id = parameter['ETL_HOST_ENDPOINT_ID']
    source_endpoint_id = parameter['SOURCE_ENDPOINT_ID']
    destination_endpoint_id = parameter['DESTINATION_ENDPOINT_ID']
    destination_path = parameter['DESTINATION_PATH']
    sql_type = parameter['SQL_TYPE'].upper().strip()
    if sql_type == 'SQL_PATH':
        sql_path = parameter['SQL_PATH']
    elif sql_type == 'SQL_SCRIPT':
        sql_script = parameter['SQL_SCRIPT']
   
    # Optional parameter
    source_endpoint_type = parameter.get('SOURCE_ENDPOINT_TYPE', 'DB')
    source_optional_arguments = parameter.get('SOURCE_OPTIONAL_ARGUMENTS', dict())
    etl_host_endpoint_type = parameter.get('ETL_HOST_ENDPOINT_TYPE', 'SSH')
    destination_endpoint_type = parameter.get('DESTINATION_ENDPOINT_TYPE', 'S3')
    
    print('=========================================')
    print('Create Connection')
    print('=========================================')
    db = select_db_helper(source_endpoint_id,etl_host_endpoint_id)

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

    output_path = f"{temp_dir}{destination_path}"
    if '.' in output_path:
        temp_dir,filename = output_path.rsplit('/',1)
        print(f"Writing file to temp directory {temp_dir}")
        print(f"Filename : {filename}")
    else:
        raise Exception('Need filename')
    
    # Make directory
    if etl_host_endpoint_type.lower() == 'ssh':
        file_manager.create_dir(f"REMOTE://{etl_host_endpoint_id}/{temp_dir}")
    else :
        raise Exception('Wrong etl_host_endpoint_type')
        
    # Export source db
    db.export_data(sql_script, output_path, optional_arguments=source_optional_arguments)
    
    # AWS CLI Command
    copy_script = f"aws s3 cp {output_path} s3://{destination_endpoint_id}/{destination_path}" # Need profile
    print('=========================================')
    print(f'Copy file from {output_path}')
    print(f'To S3 s3://{destination_endpoint_id}/{destination_path}')
    print('=========================================')
    db.db_host_ssh.send_command(copy_script)

def db_import_from_s3_job(parameter, context):

    # Required parameter
    etl_host_endpoint_id = parameter['ETL_HOST_ENDPOINT_ID']
#     source_endpoint_id = parameter['SOURCE_ENDPOINT_ID']
    s3_path = parameter['S3_PATH']
    temp_dir = parameter['TEMP_DIR']
    destination_endpoint_id = parameter['DESTINATION_ENDPOINT_ID']
    destination_table = parameter['DESTINATION_TABLE']
   
    # Optional parameter
#     source_endpoint_type = parameter.get('SOURCE_ENDPOINT_TYPE', 'S3')
#     etl_host_endpoint_type = parameter.get('ETL_HOST_ENDPOINT_TYPE', 'SSH')
#     destination_endpoint_type = parameter.get('DESTINATION_ENDPOINT_TYPE', 'DB')
    destination_optional_arguments = parameter.get('DESTINATION_OPTIONAL_ARGUMENTS', dict())
    hostname = parameter.get('HOSTNAME', None)
    overwrite = parameter.get('OVERWRITE', True)
    pre_clear_dir = parameter.get('PRE_CLEAR_DIR', False)
    post_clear_dir = parameter.get('POST_CLEAR_DIR', False)
    remove_header = parameter.get('REMOVE_HEADER', False)
    
    print('=========================================')
    print('Test Destination Connection')
    print('=========================================')
    destination_db = select_db_helper(destination_endpoint_id, etl_host_endpoint_id)
    
    print('=================================================')
    print(f'Checking file in {temp_dir}')
    print('=================================================')
    file_path =  f'REMOTE://{etl_host_endpoint_id}/{temp_dir}'
    check_directory = file_manager.check_dir(file_path)
    
    if pre_clear_dir:
        print('=================================================')
        print('Pre Clear Temp Directory')
        print('=================================================')
        file_list = file_manager.list_file(file_path)
        print(f'Delete files in {temp_dir}.')
        delete_file = file_manager.delete_file(file_path)
        print('=================================================\n')
        
    # AWS CLI Command
    download_script = f"aws s3 cp {s3_path} {temp_dir} --recursive"
    print('=========================================')
    print(f'Copy file from {s3_path}')
    print(f'To temp directory {temp_dir}')
    print('=========================================')
    destination_db.db_host_ssh.send_command(download_script)
    
    if overwrite:
        print('=================================================')
        print('Overwrite Destination Table set to True')
        print('=================================================')
        destination_db.truncate(destination_table)
    
    print('=================================================')
    print('Import')
    print('=================================================')
    for file in file_manager.list_file(file_path, show_log=False):
        print(f"From File: {file}")
        if remove_header is True:
            remove_header_script = f'echo "$(tail -n +2 {file})" > {file}'
            destination_db.db_host_ssh.send_command(remove_header_script)
        destination_db.import_data(destination_table, file, destination_optional_arguments)
    print('=================================================')

    if post_clear_dir:
        print('=================================================')
        print('Post Clear Temp Directory')
        print('=================================================')
        file_list = file_manager.list_file(file_path)
        print(f'Delete files in {temp_dir}.')
        delete_file = file_manager.delete_file(file_path)
        print('=================================================\n')