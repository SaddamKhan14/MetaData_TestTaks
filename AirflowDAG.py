#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
##
# Description:-
#
#       Script is used to schedule the PySpark Job using Airflow in accordance with
#       MetaData Data Engineer recruitment process as per below link:-
#       https://docs.google.com/forms/d/e/1FAIpQLSef0_NuUNJy-te3r5GfQx86z5PWAmZfRGlrs-YGIji7SDclYQ/viewform
##
# Development date    Developed by       Comments
# ----------------    ------------       ---------
# 14/03/2021          Saddam Khan        Initial version
#
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

"""
    Input arguments
"""
SRC_DIR = os.getcwd() + '/src/'
DAG_ID = "metadata_etl_job_scheduler"
DEFAULT_ARGS  = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 10, 12),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
  }

"""
    Init DAG object
"""
dag = DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description='Test PySpark ETL job from Airflow',
    dagrun_timeout=timedelta(minutes=60),
    schedule_interval=datetime.timedelta(days=1)
)

"""
    Defining four Bash Operator tasks :
    1. switch to source directory
    2. activate virtual environment
    3. trigger pyspark job
    4. deactivate virtual environment
"""

change_dir = BashOperator(
    task_id='change_working_directory',
    bash_command="cd " + SRC_DIR,
    dag=dag,
)

activate_env = BashOperator(
    task_id='activate_virtual_env',
    bash_command="source /usr/local/spark/bin/activate",
    dag=dag,
)

"""
    spark_submit
"""
spark_submit_cmd="/usr/local/spark/bin/spark-submit --master local elt_entrypoint.py"

spark_job = BashOperator(
    task_id='pyspark_etl_task',
    bash_command=spark_submit_cmd,
    dag=dag,
)

deactivate_env = BashOperator(
    task_id='deactivate_virtual_env',
    bash_command="deactivate",
    dag=dag,
)

"""
    Setup DAG
"""
change_dir >> activate_env >> spark_job >> deactivate_env
