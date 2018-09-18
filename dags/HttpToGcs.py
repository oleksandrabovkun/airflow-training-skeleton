import datetime as dt

from airflow import DAG

from tempfile import NamedTemporaryFile
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook


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
        endpoint=(
            "airflow-training-transform-valutas?date={{ ds }}&"
            "from=GBP&to=" + currency
        ),
        bucket="airflow_training",
        http_conn_id="airflow-training-currency-http",
        gcs_conn_id="airflow-training-storage-bucket",
        gcs_path="currency/{{ ds }}-" + currency + ".json",
        dag=dag,
    )
