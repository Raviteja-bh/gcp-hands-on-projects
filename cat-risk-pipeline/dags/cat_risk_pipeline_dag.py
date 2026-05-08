from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.apache.beam.operators.beam import (
    BeamRunPythonPipelineOperator,
)  # ← CORRECT
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowConfiguration,
)  # ← NEW
from airflow.providers.google.cloud.operators.functions import (
    CloudFunctionInvokeFunctionOperator,
)
from airflow.operators.email import EmailOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
import google.auth.transport.requests
import google.oauth2.id_token
import requests

# configuration variables
PROJECT_ID = "cat-risk-pipeline-2026"
REGION = "us-central1"
BUCKET = f"gs://cat-risk-pipeline-bucket-2026"


# default arguments for the DAG
default_args = {
    "owner": "raviteja",
    "depends_on_past": False,  # Don't wait for previous run
    "start_date": datetime(2026, 4, 23),
    "email": ["ravitejabh9790@gmail.com"],
    "email_on_failure": True,  # Email if task fails
    "email_on_retry": False,
    "retries": 3,  # Retry failed tasks 3 times
    "retry_delay": timedelta(minutes=5),  # Wait 5 mins between retries
}


# DAG definition
with DAG(
    dag_id="cat_risk_pipeline",
    default_args=default_args,
    description="Catastrophe Risk Intelligence Pipeline",
    schedule_interval="*/15 * * * *",  # Every 15 minutes
    catchup=False,  # Don't backfill missed runs
    tags=["cat-risk", "reinsurance", "streaming"],
) as dag:

    # ── TASK 1 ───────────────────────────────────────────────
    # Verify BigQuery table exists and is healthy
    check_bq_table = BigQueryCheckOperator(
        task_id="check_bq_table",
        sql="""
            SELECT COUNT(*) > 0
            FROM `cat-risk-pipeline-2026.cat_risk.events`
            WHERE DATE(timestamp) = CURRENT_DATE()
        """,
        use_legacy_sql=False,
        location="US",
        gcp_conn_id="google_cloud_default",
    )

    # ── TASK 2 ───────────────────────────────────────────────
    # Trigger Cloud Function to fetch API data
    # trigger_ingestion = CloudFunctionInvokeFunctionOperator(
    #     task_id="trigger_ingestion",
    #     function_id="fetch-cat-events",
    #     input_data={},
    #     location='US',
    #     project_id=PROJECT_ID
    # )
    def trigger_function():
        url = "https://us-central1-cat-risk-pipeline-2026.cloudfunctions.net/fetch-cat-events"

        # 1. Manually fetch the OIDC (Identity) token for the function
        auth_req = google.auth.transport.requests.Request()
        id_token = google.oauth2.id_token.fetch_id_token(auth_req, url)

        # 2. Make the HTTP POST request directly
        response = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {id_token}",
                "Content-Type": "application/json",
            },
            json={},  # Your payload
        )

        # 3. Raise an error if the call failed so Airflow retries correctly
        response.raise_for_status()
        return response.text

    trigger_ingestion = PythonOperator(
        task_id="trigger_ingestion",
        python_callable=trigger_function,
    )

    # ── TASK 3 ───────────────────────────────────────────────
    # Custom Python task — verify messages arrived in Pub/Sub
    def check_pubsub_messages():
        from google.cloud import pubsub_v1

        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(PROJECT_ID, "cat-risk-sub")
        response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": 1}
        )
        if not response.received_messages:
            raise ValueError("No messages in Pub/Sub — ingestion may have failed!")
        print(f"Messages confirmed in Pub/Sub ✅")

    verify_pubsub = PythonOperator(
        task_id="verify_pubsub", python_callable=check_pubsub_messages
    )

    # ── TASK 4 ───────────────────────────────────────────────
    # Trigger Dataflow streaming pipeline
    trigger_dataflow = BeamRunPythonPipelineOperator(  # ← CORRECT OPERATOR
        task_id="trigger_dataflow",
        execution_timeout=timedelta(minutes=10),
        runner="DataflowRunner",  # ← Run on Dataflow (not local)
        py_file=f"{BUCKET}/pipeline/dataflow_pipeline.py",
        py_options=[],
        pipeline_options={
            "project": PROJECT_ID,
            "region": REGION,
            "temp_location": f"{BUCKET}/dataflow-temp",
            "staging_location": f"{BUCKET}/dataflow-staging",
            "streaming": True,
        },
        retries=0,
        py_interpreter="python3",
        py_system_site_packages=False,
        dataflow_config=DataflowConfiguration(
            job_name="cat-risk-pipeline-{{ds_nodash}}",
            project_id=PROJECT_ID,
            location=REGION,
            wait_until_finished=False,  # Don't wait — it runs continuously
        ),
    )

    # ── TASK 5 ───────────────────────────────────────────────
    # Verify data actually landed in BigQuery
    def verify_bigquery_data(**context):
        from google.cloud import bigquery

        client = bigquery.Client()
        query = """
            SELECT COUNT(*) as event_count
            FROM `cat-risk-pipeline-2026.cat_risk.events`
            WHERE processed_at >= TIMESTAMP_SUB(
                CURRENT_TIMESTAMP(), INTERVAL 20 MINUTE
            )
        """
        result = client.query(query).result()
        count = list(result)[0]["event_count"]

        if count == 0:
            raise ValueError("No new events in BigQuery — pipeline may have failed!")

        print(f"✅ {count} events landed in BigQuery successfully")
        return count

    verify_bq_data = PythonOperator(
        task_id="verify_bq_data",
        python_callable=verify_bigquery_data,
        provide_context=True,
    )

    # ── TASK 6 ───────────────────────────────────────────────
    # Send success notification
    success_notification = EmailOperator(
        task_id="success_notification",
        to="ravitejabh9790@gmail.com",
        subject="✅ Cat Risk Pipeline — Run Successful",
        html_content="""
            <h3>Pipeline completed successfully</h3>
            <p>Run date: {{ ds }}</p>
            <p>Check dashboard: 
               <a href='your-looker-url'>Looker Studio</a>
            </p>
        """,
    )

    # Define task dependencies
    # This reads as:
    # check table → trigger ingestion → verify pubsub
    # → trigger dataflow → verify bigquery → notify
    (
        check_bq_table
        >> trigger_ingestion
        >> verify_pubsub
        >> trigger_dataflow
        >> verify_bq_data
        >> success_notification
    )
