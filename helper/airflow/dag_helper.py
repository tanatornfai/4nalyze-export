import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

import pandas as pd

from airflow.utils.task_group import TaskGroup

from helper.utility.time_helper import *
from helper.airflow.task_helper import *



def clean_list(text_list):
    return [text.strip() for text in text_list]


def extract_dag_params(row):
    row = dict(row)
    row['email_list'] = [email.strip() for email in row['email_list'].split(',')]
    row['mail_on_success'] = row.get('mail_on_success', True)
    row['retry_count'] = int(row.get('retry_count', 3))
    row['retry_delay'] = int(row.get('retry_delay', 5))
    row['zone_list'] = [clean_list(zones.split(',')) for zones in row['zone_list'].split('\n')]
    row['max_active_runs'] = int(row.get('max_active_runs', 1))
    row['default_view'] = row.get('default_view', 'graph')
    row['depends_on_past'] = row.get('depends_on_past', True)
    row['wait_for_downstream'] = row.get('wait_for_downstream', True)
    row['description'] = row.get('description', '')
    row['start_date'] = str_to_date(row['start_date'])
    if row['end_date'] is not None:
        row['end_date'] = str_to_date(row['end_date'])
    else:
        row['end_date'] = None
    row['log_endpoint_id'] = row.get('log_endpoint_id', None)
    
    default_args = {
        'owner': row['owner'],
        'default_view ': row['default_view'],
        'depends_on_past': row['depends_on_past'],
        'wait_for_downstream': row['wait_for_downstream'],
        'start_date': row['start_date'],
        'end_date': row['end_date'],
        'retries': row['retry_count'],
        'retry_delay': timedelta(minutes = row['retry_delay']),
        'do_xcom_push': False
    }
    
#     if 'end_date' is list(row.keys()):
#         row['end_date'] = str_to_date(row['end_date'])
#         default_args['end_date'] = row['end_date']
#         print(f'end_date : {end_date}')
#     else:
#         print(f'end_date not found in {row.keys()}')
        
    row['default_args'] = default_args
    
    return row


def create_zone_task(task_conf_df, zone_name, zone_dict, task_df,
                     project_name, project_id, dag_name, env, dag_owner, dag,
                     dag_retry_count = 3, dag_retry_delay = 5, dag_email_list = [], dag_mail_on_success = False,
                     log_endpoint_id = None
                    ):
    if zone_dict is None or task_df is None:
        zone_dict = dict()

        task_df = pd.DataFrame(columns = [
            'zone_name', 'sub_zone_name', 'job_name',
            'job_type','task_id',
            'task'
        ]).set_index(['zone_name', 'job_name'])
    
    with TaskGroup(zone_name) as group_task:
        zone_dict[zone_name] = group_task
        
        for sub_zone_name, sub_zone_df in task_conf_df.groupby('sub_zone_name'):
            with TaskGroup(sub_zone_name) as subgroup_task:
                
                for index, row in sub_zone_df.iterrows():
                    job_name = row['job_name']
                    job_type = row['job_type']
                    task_id = f'{job_type}_{job_name}'
                    task = create_task(task_id, row, 
                                       project_name, project_id, dag_name, env, dag_owner, dag,
                                       dag_retry_count, dag_retry_delay, dag_email_list, dag_mail_on_success,
                                       log_endpoint_id
                                      )
                    
                    task_df.loc[(zone_name, job_name), :] = [sub_zone_name, job_type, task_id, task]
                    
    return zone_dict, task_df


def create_task_relation(relation_conf_df, task_df):
    for index, row in relation_conf_df.iterrows():
        if row['parent_zone_name'] is not None and row['parent_job_name'] is not None:
            if row['parent_zone_name'] != '' and row['parent_job_name'] != '':
                child_task  = task_df.loc[(row['zone_name'], row['job_name']), 'task']
                parent_task = task_df.loc[(row['parent_zone_name'], row['parent_job_name']), 'task']
                parent_task >> child_task

                
def link_zone_list(zone_list, zone_dict):
    for i in range(len(zone_list)-1):
        parent_zone_list = [zone_dict[zone_name] for zone_name in zone_list[i]]
        child_zone_list  = [zone_dict[zone_name] for zone_name in zone_list[i + 1]]
        
        for parant_zone in parent_zone_list:
            for child_zone in child_zone_list:
                parant_zone >> child_zone

                
def create_start_end_task(zone_list, zone_dict, dag, dag_name):
    start_zone_list = [zone_dict[zone_name] for zone_name in zone_list[0]]
    end_zone_list   = [zone_dict[zone_name] for zone_name in zone_list[-1]]
    
    start_task = create_START_task(dag)
    end_task = create_END_task(dag, dag_name)
    
    start_task >> start_zone_list 
    end_zone_list >> end_task
    start_task >> end_task