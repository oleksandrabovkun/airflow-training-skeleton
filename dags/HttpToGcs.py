import datetime as dt

from airflow import DAG
import HttpToGcsOperator


dag = DAG(
    dag_id="HttpToGcs",
    schedule_interval="30 8 * * *",
    default_args={
        "owner": "airflow",
        "start_date": dt.datetime(2018, 9, 1),
        "depends_on_past": True,
        "email_on_failure": True,
        "email": "airflow_errors@myorganisation.com",
    },
)

for currency in {"EUR", "USD"}:
    HttpToGcsOperator(
        task_id="get_currency_" + currency,
        method="GET",
        endpoint="airflow-training-transform-valutas?date={{ ds }}&from=GBP&to="
        + currency,
        http_conn_id="airflow-training-currency-http",
        gcs_conn_id="airflow-training-storage-bucket",
        gcs_path="currency/{{ ds }}-" + currency + ".json",
        dag=dag,
    )
