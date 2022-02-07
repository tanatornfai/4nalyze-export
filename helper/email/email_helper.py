import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

import pwd
import os

from datetime import datetime
from pytz import timezone
from airflow.utils.email import send_email_smtp

# link folder /opt/social/task/dag/template to /usr/local/airflow/dag/template
_email_template_folder = f'{root_path}template/'



def get_email_template(template):
    content = open(f'{_email_template_folder}{template}.html').read()
    return content

def send_email(context, email_to, html_template, email_title, header, task_log = "None"):
    # Header
    context['header'] = header
    
    # Render Template
    render_template = context['task'].render_template

    # Set Task Log
    context['task_log'] = task_log

    # Get HTML Content
    print(f'Get HTML Template : {html_template}')
    html_template = get_email_template(template = html_template)
    html_content = render_template(html_template, context)

    # Get Title Content
    print(f'Email Title : {email_title}')
    title_content = render_template(email_title, context)

    # Send Email
    print(f'Send to : {email_to}')
    send_email_smtp(email_to, title_content, html_content)
    

def custom_email_callback(email_to=[], task_log=None, attachment_files=[], on_success=True, on_failed=True, on_retry=True, log_context=False):
    """ Return multiple functions to send email from template. See usage below.
        Arguments: 
            email_to (str,list) - List of receiver emails.
            task_log (any,jinja) - Any object you want to log in email (e.g. Parameters).
            attachment_file (list,jinja) - local path of attachment files. Can be jinja template.
            on_success/on_failed/on_retry (bool) - To send email or not by task status.
        Usage:
            Pass as arguments in operator. e.g. ``PapermillOperator(..., **custom_email_callback(...))``
    """
    def send_custom_success_email(context):
        render_template = context['task'].render_template

        context.update(dict(
            task_log=render_template("None" if task_log is None else str(task_log), context) + ("\n[context]\n%s" % str(context)if log_context else ""),
            try_number=context['task_instance'].prev_attempted_tries,
            max_tries=context['task_instance'].max_tries,
        ))
        title = render_template('Airflow - Job: {{ti.dag_id}} Task: {{ti.task_id}} is SUCCESS', context)
        content = get_email_template(template = 'EMAIL_SUCCESS_TEMPLATE_MIN')
        files = [render_template(f, context) for f in attachment_files]

        send_email_smtp(email_to, title, render_template(content, context), files=files)

    def send_custom_retry_email(context):
        render_template = context['task'].render_template

        context.update(dict(
            task_log=render_template("None" if task_log is None else str(task_log), context) + ("\n[context]\n%s" % str(context)if log_context else ""),
            try_number=context['task_instance'].prev_attempted_tries,
            max_tries=context['task_instance'].max_tries,
        ))
        title = render_template('Airflow - Job: {{ti.dag_id}} Task: {{ti.task_id}} is RETRYING', context)
        content = get_email_template(template = 'EMAIL_RETRY_TEMPLATE_MIN')
        files = [render_template(f, context) for f in attachment_files]

        send_email_smtp(email_to, title, render_template(content, context), files=files)

    def send_custom_failed_email(context):
        render_template = context['task'].render_template
        print(context)
        context.update(dict(
            task_log=render_template("None" if task_log is None else str(task_log), context) + ("\n[context]\n%s" % str(context)if log_context else ""),
            try_number=context['task_instance'].prev_attempted_tries,
            max_tries=context['task_instance'].max_tries,
        ))
        title = render_template('Airflow - Job: {{ti.dag_id}} Task: {{ti.task_id}} is FAILED', context)
        content = get_email_template(template = 'EMAIL_FAILED_TEMPLATE_MIN')
        files = [render_template(f, context) for f in attachment_files]

        send_email_smtp(email_to, title, render_template(content, context), files=files)
    
    return dict(
        on_failure_callback=send_custom_failed_email if on_failed else None,
        on_retry_callback=send_custom_retry_email if on_retry else None,
        on_success_callback=send_custom_success_email if on_success else None,
    )