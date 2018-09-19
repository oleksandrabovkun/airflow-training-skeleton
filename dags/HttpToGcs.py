import datetime as dt

from airflow import DAG

from tempfile import NamedTemporaryFile
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataProcPySparkOperator,
    DataprocClusterDeleteOperator,
)
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from godatadriven.operators.postgres_to_gcs import (
    PostgresToGoogleCloudStorageOperator
)
from godatadriven.operators.gcs_to_bq import (
    GoogleCloudStorageToBigQueryOperator
)


class HttpToGcsOperator(BaseOperator):
    """
    Calls an endpoint on an HTTP system to execute an action
    :param http_conn_id: The connection to run the operator against
    :type http_conn_id: string
    :param endpoint: The relative part of the full url. (templated)
    :type endpoint: string
    :param gcs_path: The path of the GCS to store the result
    :type gcs_path: string
    """

    template_fields = ("endpoint", "gcs_path")
    template_ext = ()
    ui_color = "#f4a460"

    @apply_defaults
    def __init__(
        self,
        endpoint,
        gcs_path,
        method="GET",
        http_conn_id="http_default",
        gcs_conn_id="gcs_default",
        bucket="airflow_training",
        *args,
        **kwargs
    ):
        super(HttpToGcsOperator, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.method = method
        self.endpoint = endpoint
        self.gcs_path = gcs_path
        self.gcs_conn_id = gcs_conn_id
        self.bucket = bucket

    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)
        self.log.info("Calling HTTP method")
        response = http.run(self.endpoint)
        with NamedTemporaryFile() as tmp_file_handle:
            tmp_file_handle.write(response.content)
            tmp_file_handle.flush()
            hook = GoogleCloudStorageHook(
                google_cloud_storage_conn_id=self.gcs_conn_id
            )
            hook.upload(
                bucket=self.bucket,
                object=self.gcs_path,
                filename=tmp_file_handle.name,
            )


dag = DAG(
    dag_id="HttpToGcs",
    schedule_interval="30 8 * * *",
    default_args={
        "owner": "airflow",
        "start_date": dt.datetime(2018, 9, 17),
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

dataproc_create_cluster = DataprocClusterCreateOperator(
    task_id="create_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id="gdd-05b583b94256b6965bb8c8119a",
    num_workers=2,
    zone="europe-west4-a",
    dag=dag,
)

dataflow_job = DataFlowPythonOperator(
    task_id="land_registry_prices_to_bigquery",
    dataflow_default_options={
        "project": "gdd-05b583b94256b6965bb8c8119a",
        "region": "europe-west1",
    },
    py_file="gs://airflow_training/other/dataflow_job.py",
    dag=dag,
)

for currency in {"EUR", "USD"}:
    HttpToGcsOperator(
        task_id="get_currency_" + currency,
        method="GET",
        endpoint=(
            "airflow-training-transform-valutas?date={{ ds }}&"
            "from=GBP&to=" + currency
        ),
        bucket="airflow_training",
        http_conn_id="airflow-training-currency-http",
        gcs_conn_id="airflow-training-storage-bucket",
        gcs_path="currency/{{ ds }}-" + currency + ".json",
        dag=dag,
    ) >> dataproc_create_cluster, dataflow_job


compute_aggregates = DataProcPySparkOperator(
    task_id="compute_aggregates",
    main="gs://airflow_training/build_statistics.py",
    cluster_name="analyse-pricing-{{ ds }}",
    arguments=["{{ ds }}"],
    dag=dag,
)


dataproc_delete_cluster = DataprocClusterDeleteOperator(
    task_id="delete_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    dag=dag,
    project_id="gdd-05b583b94256b6965bb8c8119a",
    trigger_rule=TriggerRule.ALL_DONE,
)


gcs_to_bigquery = GoogleCloudStorageToBigQueryOperator(
    task_id="write_to_bq",
    bucket="airflow_training",
    source_objects=["average_prices/transfer_date={{ ds }}/*"],
    destination_project_dataset_table=(
        "gdd-05b583b94256b6965bb8c8119a:"
        "prices.land_registry_price${{ ds_nodash }}"
    ),
    source_format="PARQUET",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)


pgsq_to_gcs >> dataproc_create_cluster
pgsq_to_gcs >> dataflow_job
dataproc_create_cluster >> compute_aggregates >> dataproc_delete_cluster
compute_aggregates >> gcs_to_bigquery
