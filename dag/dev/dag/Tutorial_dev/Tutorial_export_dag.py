import sys, os
sys.path.insert(0, '/usr/local/airflow/')

from airflow import DAG

from helper.file import file_manager

from helper.airflow.dag_helper import *



###############################################################################
# DAG ID
###############################################################################
project_name = 'Tutorial'
env = 'dev'
dag_name = 'export'
project_id = f'{project_name}_{env}'
dag_id = f'{dag_name}_{env}'
filter_zone_name = None



###############################################################################
# Get DAG Parameters
###############################################################################
user_path = os.path.dirname(os.path.abspath(__file__)).split('dags/dag/')[1].split('/')[0]
dag_params = file_manager.read_dag_conf(project_id, project_name, dag_name, user_path)
dag_params = extract_dag_params(dag_params)

# Dag Config
default_args         = dag_params['default_args']
start_date           = dag_params['start_date']
end_date             = dag_params['end_date']
description          = dag_params['description']
schedule_interval    = dag_params['schedule_interval']
max_active_runs      = dag_params['max_active_runs']
default_view         = dag_params['default_view']
dag_owner            = dag_params['owner']
schedule_description = dag_params['schedule_description']
log_endpoint_id      = dag_params['log_endpoint_id']
tags = [project_name, env, dag_name, dag_owner, schedule_description]
tags = list(set([tag for tag in tags if tag is not None and len(tag) > 0]))


# Default Config
dag_retry_count      = dag_params['retry_count']
dag_retry_delay      = dag_params['retry_delay']
dag_email_list       = dag_params['email_list']
dag_mail_on_success  = dag_params['mail_on_success']


# Zone
zone_list            = dag_params['zone_list']


if filter_zone_name is not None:
    dag_id           = f'{dag_id}_{filter_zone_name}'
    zone_list        = [zone for zone in zone_list if filter_zone_name in zone]
    tags             = tags + [filter_zone_name]
    
    

###############################################################################
# DAG
###############################################################################
zone_dict = None
task_df = None

with DAG(dag_id, 
         default_args = default_args, 
         start_date = start_date,
         end_date = end_date,
         description = description,
         tags = tags, 
         schedule_interval = schedule_interval,
         max_active_runs = max_active_runs, 
         default_view = default_view
        ) as dag:

    for zones in zone_list:
        for zone_name in zones:
            # Get Zone Config
            task_conf_df, relation_conf_df = file_manager.read_zone_conf(project_id, project_name, dag_name, zone_name, user_path)
            
            # Create Task
            zone_dict, task_df = create_zone_task(task_conf_df, zone_name, zone_dict, task_df,
                                                  project_name, project_id, dag_name, env, dag_owner, dag,
                                                  dag_retry_count, dag_retry_delay, dag_email_list, dag_mail_on_success,
                                                  log_endpoint_id)
            # Create Relation
            create_task_relation(relation_conf_df, task_df)
            
    # Link Zone
    link_zone_list(zone_list, zone_dict)
    
    # Start End Task
    create_start_end_task(zone_list, zone_dict, dag, dag_name)