from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessStartJobOperator,
    EmrServerlessDeleteApplicationOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrServerlessJobSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from python_scripts.redshift.staging_layer import load_to_staging, clear_staging_tables
from python_scripts.redshift.fill_dim_customers import dim_customers
from python_scripts.redshift.fill_dim_date import dim_date
from python_scripts.redshift.fill_dim_products import dim_products
from python_scripts.redshift.fill_dim_staffs import dim_staffs
from python_scripts.redshift.fill_dim_stores import dim_stores
from python_scripts.redshift.fill_fact_sales import fact_sales

from datetime import datetime, timedelta
import pendulum
import logging


# setting log level
logging.basicConfig(level=logging.INFO)


def utc_to_bkk_timezone(datetime_utc: datetime) -> str:
    """
    Convert timezone from UTC to Bangkok.

    :param datetime_utc: Datetime in UTC timezone.
    """

    # convert datetime to pendulum datetime
    pendulum_dt = pendulum.instance(datetime_utc)

    # convert to Bangkok timezone
    datetime_bkk = pendulum_dt.in_timezone('Asia/Bangkok')

    # format datetime to str
    datetime_str = datetime_bkk.format('YYYY-MM-DD')

    return datetime_str


def previous_dag_execution_date(prev_execution_date_success: datetime) -> str:
    """
    Get previous execution date of dag where state is success.

    :param prev_execution_date_success: Airflow context of previous success execution date.
    """

    # if previous dag execution date exists
    if prev_execution_date_success:

        # convert timezone
        previous_execution_date_str = utc_to_bkk_timezone(prev_execution_date_success)

        logging.info(f'previous execution date is: {previous_execution_date_str}')

        return previous_execution_date_str

    # if there is no previous dag execution date
    else:
        
        logging.info('no previous execution date, default set to: 2015-12-31')

        return '2015-12-31'


def current_dag_execution_date(execution_date: datetime) -> str:
    """
    Get current execution date of dag.

    :param execution_date: Airflow context of current execution date.
    """

    # convert timezone
    current_execution_date_str = utc_to_bkk_timezone(execution_date)

    logging.info(f'current execution date is: {current_execution_date_str}')

    return current_execution_date_str


# settings for DAG
default_args = {
    'owner': 'pradpradprad',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 10, 9, tz='Asia/Bangkok'),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'dagrun_timeout': timedelta(minutes=30)
}


# define current time
current_time = pendulum.now('Asia/Bangkok')
data_record_date = current_time.subtract(days=1)
year = data_record_date.year
month = f'{data_record_date.month:02}'
day = f'{data_record_date.day:02}'

# define file path and name
file_path = f'{year}/{month}/{day}'
file_name = f'{year}{month}{day}'

# define bucket name
raw_bucket = Variable.get('raw_bucket_name')
processed_bucket = Variable.get('processed_bucket_name')
log_bucket = Variable.get('log_bucket_name')
source_bucket = Variable.get('source_bucket_name')

# define IAM role
emr_role_arn = Variable.get('emr_runtime_role_arn')
redshift_role_arn = Variable.get('redshift_role_arn')


# define DAG
with DAG(
    'pipeline_dag',
    description='retail store ETL pipeline',
    default_args=default_args,
    schedule_interval='@daily'
) as dag:
    

    # setup etl date task group
    with TaskGroup('get_etl_date') as get_etl_date:

        previous_etl_date = PythonOperator(
            task_id='previous_etl_date',
            python_callable=previous_dag_execution_date
        )

        current_etl_date = PythonOperator(
            task_id='current_etl_date',
            python_callable=current_dag_execution_date
        )

        previous_etl_date >> current_etl_date


    # data extraction task group
    with TaskGroup('extract_data') as extract_data:

        # extract order table
        extract_order = PostgresOperator(
            task_id='extract_order',
            postgres_conn_id='rds_conn',
            sql='sql_scripts/extraction/extract_order.sql',
            params={
                'bucket': raw_bucket,
                'file_path': file_path,
                'file_name': file_name
            }
        )

        # extract customer table
        extract_customer = PostgresOperator(
            task_id='extract_customer',
            postgres_conn_id='rds_conn',
            sql='sql_scripts/extraction/extract_customer.sql',
            params={
                'bucket': raw_bucket,
                'file_path': file_path,
                'file_name': file_name
            }
        )

        # extract product table
        extract_product = PostgresOperator(
            task_id='extract_product',
            postgres_conn_id='rds_conn',
            sql='sql_scripts/extraction/extract_product.sql',
            params={
                'bucket': raw_bucket,
                'file_path': file_path,
                'file_name': file_name
            }
        )

        # extract staff table
        extract_staff = PostgresOperator(
            task_id='extract_staff',
            postgres_conn_id='rds_conn',
            sql='sql_scripts/extraction/extract_staff.sql',
            params={
                'bucket': raw_bucket,
                'file_path': file_path,
                'file_name': file_name
            }
        )

        # extract store table
        extract_store = PostgresOperator(
            task_id='extract_store',
            postgres_conn_id='rds_conn',
            sql='sql_scripts/extraction/extract_store.sql',
            params={
                'bucket': raw_bucket,
                'file_path': file_path,
                'file_name': file_name
            }
        )


    # data transformation task group
    with TaskGroup('transform_data') as transform_data:

        # create spark application
        create_emr_spark_app = EmrServerlessCreateApplicationOperator(
            task_id='create_emr_spark_app',
            release_label='emr-7.1.0',
            job_type='SPARK',
            config={
                'name': 'emr-serverless-transformation',
                'maximumCapacity': {
                    'cpu': '8vCPU',
                    'memory': '32GB',
                    'disk': '50GB'
                },
                'autoStopConfiguration': {
                    'enabled': True,
                    'idleTimeoutMinutes': 5
                },
                'monitoringConfiguration': {
                    's3MonitoringConfiguration': {
                        'logUri': f's3://{log_bucket}/emr-log/'
                    }
                }
            },
            waiter_delay=10
        )

        # task returned application id
        application_id = create_emr_spark_app.output

        # start pyspark job in our created application
        spark_submit_job = EmrServerlessStartJobOperator(
            task_id='spark_submit_job',
            application_id=application_id,
            execution_role_arn=emr_role_arn,
            job_driver={
                'sparkSubmit': {
                    'entryPoint': f's3://{source_bucket}/script/spark_job.py',
                    'entryPointArguments': [raw_bucket, file_name, processed_bucket]
                }
            },
            name='ETL_Transformation',
            waiter_delay=10
        )

        # task returned job id
        job_id = spark_submit_job.output

        # check if the running job is finished
        check_job_status = EmrServerlessJobSensor(
            task_id='check_job_status',
            application_id=application_id,
            job_run_id=job_id
        )

        # delete application after finished spark job
        delete_spark_app = EmrServerlessDeleteApplicationOperator(
            task_id='delete_spark_app',
            application_id=application_id,
            waiter_delay=10
        )

        create_emr_spark_app >> spark_submit_job >> check_job_status >> delete_spark_app


    # data loading task group
    with TaskGroup('load_data') as load_data:

        # load processed data from s3 into staging layer
        load_staging_layer = SQLExecuteQueryOperator(
            task_id='load_staging_layer',
            sql=load_to_staging,
            conn_id='redshift_conn',
            params={
                'bucket': processed_bucket,
                'path': file_path,
                'iam_role': redshift_role_arn
            }
        )

        # load data from staging layer to serving layer

        load_dim_customers = SQLExecuteQueryOperator(
            task_id='load_dim_customers',
            sql=dim_customers,
            conn_id='redshift_conn'
        )

        load_dim_date = SQLExecuteQueryOperator(
            task_id='load_dim_date',
            sql=dim_date,
            conn_id='redshift_conn'
        )

        load_dim_products = SQLExecuteQueryOperator(
            task_id='load_dim_products',
            sql=dim_products,
            conn_id='redshift_conn'
        )

        load_dim_staffs = SQLExecuteQueryOperator(
            task_id='load_dim_staffs',
            sql=dim_staffs,
            conn_id='redshift_conn'
        )

        load_dim_stores = SQLExecuteQueryOperator(
            task_id='load_dim_stores',
            sql=dim_stores,
            conn_id='redshift_conn'
        )

        load_fact_sales = SQLExecuteQueryOperator(
            task_id='load_fact_sales',
            sql=fact_sales,
            conn_id='redshift_conn'
        )

        clear_staging_layer = SQLExecuteQueryOperator(
            task_id='clear_staging_layer',
            sql=clear_staging_tables,
            conn_id='redshift_conn'
        )

        load_staging_layer >> [
            load_dim_customers, load_dim_date,
            load_dim_products, load_dim_staffs, load_dim_stores
        ] >> load_fact_sales >> clear_staging_layer


    # dependency
    get_etl_date >> extract_data >> transform_data >> load_data