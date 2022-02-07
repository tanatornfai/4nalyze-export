import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

from helper.email.email_helper import send_email
from helper.utility.param_helper import read_parameter



def send_email_task(parameter, **context):
    send_email_job(parameter, context)
    
def send_email_job(parameter, context):
    parameter = read_parameter(parameter, context)
    
    # Required parameter
    email_to = parameter['EMAIL_TO']
    html_template = parameter['HTML_TEMPLATE']
    email_title = parameter['EMAIL_TITLE']
    header = parameter['HEADER']

    # Optional parameter
    task_log = parameter.get('TASL_LOG', 'NONE')
    
    # Edit to receive value from XCOM
    # Not implement yet
    
    
    # Send Email
    send_email(context, email_to, html_template, email_title, header, task_log = "None")
