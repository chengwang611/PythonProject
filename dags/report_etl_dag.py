"""
Airflow DAG for running daily report ETL pipelines.

Schedule:
- Customer Report: 9:00 AM UTC daily
- Inventory Report: After customer_report succeeds
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import logging
from pyspark.sql import SparkSession

# Import report runner
from src.etl.reports.report_runner import main as run_report

logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'report_etl_pipeline',
    default_args=default_args,
    description='Daily ETL pipeline for customer and inventory reports',
    schedule_interval='0 9 * * *',  # 9:00 AM UTC every day
    catchup=False,
    tags=['reports', 'etl', 'daily'],
)


def get_spark_session():
    """Create and return a SparkSession."""
    spark = SparkSession.builder \
        .appName("ReportETL") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.default.parallelism", "200") \
        .getOrCreate()
    return spark


def get_mssql_config():
    """Get MSSQL configuration from Airflow variables or environment."""
    mssql_config = {
        'server': Variable.get('mssql_server', default_var='localhost'),
        'database': Variable.get('mssql_database', default_var='reports_db'),
        'user': Variable.get('mssql_user', default_var='sa'),
        'password': Variable.get('mssql_password', default_var=''),
        'port': Variable.get('mssql_port', default_var='1433'),
    }
    return mssql_config


def run_customer_report(**context):
    """Run customer report processor."""
    try:
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        logger.info(f"Starting customer report for date: {execution_date}")

        spark = get_spark_session()
        mssql_config = get_mssql_config()

        run_report(
            report_name='customer_report',
            report_date=execution_date,
            spark=spark,
            mssql_config=mssql_config
        )

        logger.info(f"Successfully completed customer report for date: {execution_date}")
        context['task_instance'].xcom_push(key='customer_report_status', value='success')

    except Exception as e:
        logger.error(f"Failed to run customer report: {str(e)}")
        raise


def run_inventory_report(**context):
    """Run inventory report processor."""
    try:
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        logger.info(f"Starting inventory report for date: {execution_date}")

        spark = get_spark_session()
        mssql_config = get_mssql_config()

        run_report(
            report_name='inventory_report',
            report_date=execution_date,
            spark=spark,
            mssql_config=mssql_config
        )

        logger.info(f"Successfully completed inventory report for date: {execution_date}")
        context['task_instance'].xcom_push(key='inventory_report_status', value='success')

    except Exception as e:
        logger.error(f"Failed to run inventory report: {str(e)}")
        raise


# Define tasks
customer_report_task = PythonOperator(
    task_id='run_customer_report',
    python_callable=run_customer_report,
    provide_context=True,
    dag=dag,
)

inventory_report_task = PythonOperator(
    task_id='run_inventory_report',
    python_callable=run_inventory_report,
    provide_context=True,
    dag=dag,
)

# Set dependencies: inventory_report runs after customer_report succeeds
customer_report_task >> inventory_report_task

