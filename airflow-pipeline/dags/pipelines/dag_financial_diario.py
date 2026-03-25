from airflow import DAG
from airflow.version import version
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from constructors.pipeline_dag_creator import PipelineDagCreator
from pipelines.parameters.financial_diario.integration.jobs import *
from pipelines.parameters.financial_diario.business.jobs import *
from pipelines.parameters.financial_diario.pipeline import pipeline

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
with DAG('AmpliFinanceiro',
         start_date=datetime(2021,8,11),
         max_active_runs=3,
         schedule_interval='59 14 2 * *',  # (Cron Diário) https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
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

    #Integration
    pdc.add_job('Integration', coupon, 'RW')
    pdc.add_job('Integration', course, 'RW')
    pdc.add_job('Integration', enrollment_course, 'RW')
    pdc.add_job('Integration', enrollment_reason, 'RW')
    pdc.add_job('Integration', financial_instalment_charge, 'RW')
    pdc.add_job('Integration', financial_instalment, "RW")
    pdc.add_job('Integration', financial_payment_plan, 'RW')
    pdc.add_job('Integration', locale_city, 'RW')
    pdc.add_job('Integration', locale_state, 'RW')
    pdc.add_job('Integration', payment_charge, 'RW')
    pdc.add_job('Integration', payment_credit_card, 'RW')
    pdc.add_job('Integration', student, 'RW')

    #Business
    pdc.add_job('Business', alunado, 'Ampli')
    pdc.add_job('Business', vpex_nfse_itens, 'Ampli')
    pdc.add_job('Business', vpex_abandono, 'Ampli')
    pdc.add_job('Business', vpex_contas_a_receber, 'Ampli')
    pdc.add_job('Business', vpex_faturamento, 'Ampli')
    pdc.add_job('Business', vpex_nfse_capa, 'Ampli')
    pdc.add_job('Business', vpex_recebimento, 'Ampli')
    

    #pdc.add_job('Business', job_rent_user_mensal,'Mock')

    pipeline_task_group = pdc.create_pipeline_task_group()
    
    t_start >> pipeline_task_group >> t_end
    
