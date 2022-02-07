import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

import pytz

from datetime import datetime,timedelta
from dateutil.relativedelta import *

default_timezone = 'Asia/Bangkok'
default_date_format = '%Y-%m-%d'
default_time_format = '%H-%M-%S'
default_datetime_format = '%s_%s' % (default_date_format, default_time_format)
dst_tz = pytz.timezone('Asia/Bangkok')
offset_hour = int(datetime.now(dst_tz).utcoffset().total_seconds()/3600)


def get_process_date(datetime_format = default_datetime_format):
    return "{{ (execution_date + macros.timedelta(hours=%d)).strftime('%s') }}" % (offset_hour, datetime_format)

def get_current_timestamp(str_format = '%Y-%m-%d %H:%M:%S', to_str = True):
    timestamp = datetime.now(pytz.timezone('Asia/Bangkok'))
    if to_str:
        timestamp = time_to_str(timestamp, str_format)
    return timestamp


def time_to_str(timestamp, str_format = '%Y-%m-%d %H:%M:%S'):
    return timestamp.strftime(str_format)


def str_to_time(timestamp_str, str_format = '%Y-%m-%d %H:%M:%S'):
    return datetime.strptime(timestamp_str, str_format)


def str_to_date(timestamp_str, str_format = '%Y-%m-%d'):
    return datetime.strptime(timestamp_str, str_format)


def get_multi_datetime_context(context, ASAT_DT_FLAG = 'PROCESS_DATE'):
    execution_date = context['execution_date'].astimezone(dst_tz)
    process_date =  context['next_execution_date'].astimezone(dst_tz)
    
    first_date = process_date.replace(day = 1)
    last_date = process_date.replace(day = 1) - timedelta(days = 1)
    
    ASAT_DT_FLAG = str(ASAT_DT_FLAG).upper()
    if ASAT_DT_FLAG in ['PROCESS_DATE', 'TODAY', 'NONE']:
        asat_dt = process_date
    elif ASAT_DT_FLAG == 'EXECUTION_DATE':
        asat_dt = execution_date
    elif ASAT_DT_FLAG == 'FIRST_DATE':
        asat_dt = first_date
    elif ASAT_DT_FLAG == 'LAST_DATE' :
        asat_dt = last_date
        
    asat_dt = time_to_str(asat_dt, str_format = '%Y%m%d')
    now = get_current_timestamp()
    return asat_dt, process_date.date(), execution_date.date(), first_date.date(), last_date.date(), now

def get_multi_datetime_jinja(parameter, ASAT_DT_FLAG = 'PROCESS_DATE'):
    execution_date = parameter['EXECUTION_DATE']
    execution_date = str_to_time(execution_date, str_format = '%Y-%m-%dT%H:%M:%S%z').astimezone(dst_tz)
    
    process_date = parameter['PROCESS_DATE']
    process_date = str_to_time(process_date, str_format = '%Y-%m-%dT%H:%M:%S%z').astimezone(dst_tz)
    
    first_date = process_date.replace(day = 1)
    last_date = process_date.replace(day = 1) - timedelta(days = 1)
    
    ASAT_DT_FLAG = str(ASAT_DT_FLAG).upper()
    if ASAT_DT_FLAG in ['PROCESS_DATE', 'TODAY', 'NONE']:
        asat_dt = process_date
    elif ASAT_DT_FLAG == 'EXECUTION_DATE':
        asat_dt = execution_date
    elif ASAT_DT_FLAG == 'FIRST_DATE':
        asat_dt = first_date
    elif ASAT_DT_FLAG == 'LAST_DATE' :
        asat_dt = last_date
        
    asat_dt = time_to_str(asat_dt, str_format = '%Y%m%d')
    now = get_current_timestamp()
    return asat_dt, process_date.date(), execution_date.date(), first_date.date(), last_date.date(), now