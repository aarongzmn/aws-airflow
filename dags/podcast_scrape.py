from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    "owner": "airflow",
    "depend_on_past": False,
    "start_date": datetime(2020, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=20),
}


def get_activated_sources():
    request = """
        SELECT id, feed_url, bucket_directory
        FROM podcasts
        WHERE bucket_sync = True
    """
    pg_hook = PostgresHook(postgres_conn_id="aurora_podcastdb")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    sources = cursor.fetchall()
    for source in sources:
        print(f"Source: {source[0]} - activated: {source[1]}")
    return sources


with DAG(
    "podcast_scrape",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False
) as dag:

    start_task = DummyOperator(task_id="start_task")
    hook_task = PythonOperator(
        task_id="hook_task",
        python_callable=get_activated_sources
    )

    start_task >> hook_task
