import datetime as dt

from airflow import DAG
from godatadriven.operators.postgres_to_gcs import (
    PostgresToGoogleCloudStorageOperator
)


dag = DAG(
    dag_id="postgressToGcs",
    schedule_interval="30 7 * * *",
    default_args={
        "owner": "airflow",
        "start_date": dt.datetime(2018, 9, 16),
        "depends_on_past": True,
        "email_on_failure": True,
        "email": "airflow_errors@myorganisation.com",
    },
)

pgsq_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="postgres_to_gcs",
    postgres_conn_id="postgres_training",
    sql=(
        "SELECT * FROM land_registry_price_paid_uk "
        "WHERE transfer_date = '{{ ds }}'"
    ),
    bucket="airflow_training",
    filename="land_registry_price_paid_uk/{{ ds }}/properties_{}.json",
    dag=dag,
)
