from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    "owner": "airflow",
    "depend_on_past": False,
    "start_date": datetime(2020, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


def get_podcast_list():
    query = """
        SELECT id, feed_url, bucket_directory
        FROM podcasts
        WHERE bucket_sync = True
    """
    postgres = PostgresHook(postgres_conn_id="aws_podcastdb").get_conn()
    with postgres.cursor(name="serverCursor", cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        query_result = cur.fetchall()
        podcast_list = [{k:v for k, v in row.items()} for row in query_result]
        podcast_list = [dict(row) for row in data]  # same as above but may not serializable...
        print(f"Found {len(podcast_list)} podcasts to update: {podcast_list}")
    postgres.close()
    return podcast_list


with DAG(
    "podcast_scrape",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False
) as dag:

    start_task = DummyOperator(
        task_id="start_task"
    )
    hook_task = PythonOperator(
        task_id="hook_task",
        python_callable=get_podcast_list
    )

    start_task >> hook_task
