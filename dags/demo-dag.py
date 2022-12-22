from airflow import DAG
from airflow.operators.bash  import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

seven_days_ago = datetime.combine(datetime.today() - timedelta(1),
        datetime.min.time())
def slack_notification(context):
    slack_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
        )
    failed_alert = SlackWebhookOperator(
        task_id='slack_notification',
        http_conn_id='slack_webhook',
        message=slack_msg)
    return failed_alert.execute(context=context)

default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2022,5,23),
        'schedule_interval':"@daily",
        'email': ['bsmarty978@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'catchup':False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback':slack_notification,
        }

dag = DAG('Demo-Dag1', default_args=default_args)
t1 = BashOperator(
            task_id='task_1',
                # bash_command='python3 /home/ubuntu/test-scripts/testcode.py',
                bash_command='echo Helllo From EKS Airflow',
                    dag=dag)
t1