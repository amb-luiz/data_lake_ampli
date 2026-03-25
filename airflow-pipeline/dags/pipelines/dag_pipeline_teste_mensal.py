from airflow import DAG
from airflow.version import version
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from constructors.pipeline_dag_creator import PipelineDagCreator
from pipelines.parameters.project_datapipeline_mensal.integration.jobs import *
from pipelines.parameters.project_datapipeline_mensal.business.jobs import *
from pipelines.parameters.project_datapipeline_mensal.pipeline import pipeline

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('PipelineTesteMensal',
         start_date=datetime(2021,8,11),
         max_active_runs=3,
         schedule_interval=None,  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
         default_args=default_args,
         catchup=False # enable if you don't want historical dag runs to run
         ) as dag:

    t_start = DummyOperator(
        task_id='start'
    )
    
    t_end = DummyOperator(
        task_id='end'
    )
    pdc = PipelineDagCreator(pipeline, dag)
    pdc.add_job('Integration', job_user_mensal, 'Mock')
    
    pdc.add_job('Integration', job_rent_mensal, 'Mock')
    #pdc.add_job('Integration',job_json)
    pdc.add_job('Business', job_rent_user_mensal,'Mock')
    #pdc.add_job('Business', job_json_transform)
     
    pipeline_task_group = pdc.create_pipeline_task_group()
    
    t_start >> pipeline_task_group >> t_end
    
