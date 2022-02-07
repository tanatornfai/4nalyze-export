import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

import time

import pandas as pd
import numpy as np

from urllib.parse import quote  
from sqlalchemy import create_engine, text

from helper.shell.ssh_helper import ssh_helper
from helper.file import file_manager

import sqlalchemy
# import pyodbc

import re
from datetime import datetime
from helper.file import file_manager
from helper.database.mssql_helper import mssql_helper
from helper.database.mysql_helper import mysql_helper
from helper.database.postgresql_helper import postgresql_helper
from helper.utility.param_helper import read_parameter


def select_db_helper(endpoint_id, host_endpoint_id=None, print_log=True):
    endpoint_type = 'DB'
    if print_log is True:
        print(f'Creating {endpoint_type.upper()} {endpoint_id} Endpoint')
    
    if print_log is True:
        print('\tGetting Credential')
    row = file_manager.get_credential(endpoint_type, endpoint_id, validate_endpoint = True, print_log=print_log)
    
    if row['database_type'].lower() == 'mssql':
        return mssql_helper(row, endpoint_id, host_endpoint_id, print_log=print_log)
    elif row['database_type'].lower() == 'mysql':
        return mysql_helper(row, endpoint_id, host_endpoint_id, print_log=print_log)
    elif row['database_type'].lower() == 'postgresql':
        return postgresql_helper(row, endpoint_id, host_endpoint_id)
    else:
        raise Exception('Database Type Error!!!')
    