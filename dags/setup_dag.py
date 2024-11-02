from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable

from datetime import timedelta
import pendulum
from python_scripts.source_table_setup import transform_csv
from python_scripts.redshift.redshift_table import setup_tables


# settings for DAG
default_args = {
    'owner': 'pradpradprad',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 6, 1, tz='Asia/Bangkok'),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'dagrun_timeout': timedelta(minutes=30)
}


# define source bucket name
source_bucket = Variable.get('source_bucket_name')


# define DAG
with DAG(
    'setup_dag',
    description='prepare data source for data pipeline',
    default_args=default_args,
    schedule_interval=None
) as dag:
    
    # transform csv files and save to s3 bucket
    transform_csv = PythonOperator(
        task_id='transform_csv',
        python_callable=transform_csv,
        op_kwargs={
            'source_bucket': source_bucket
        }
    )

    # create tables
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='rds_conn',
        sql='sql_scripts/rds/rds_table.sql'
    )

    # import data from s3 to RDS
    import_to_rds = PostgresOperator(
        task_id='import_to_rds',
        postgres_conn_id='rds_conn',
        sql='sql_scripts/rds/import_to_rds.sql',
        params={'bucket_name': source_bucket}
    )

    # create trigger for tables
    create_trigger = PostgresOperator(
        task_id='create_trigger',
        postgres_conn_id='rds_conn',
        sql='sql_scripts/rds/rds_trigger.sql'
    )

    # create redshift schemas and tables
    redshift_setup = SQLExecuteQueryOperator(
        task_id='redshift_setup',
        sql=setup_tables,
        conn_id='redshift_conn'
    )

    # dependency
    transform_csv >> create_table >> import_to_rds >> create_trigger >> redshift_setup