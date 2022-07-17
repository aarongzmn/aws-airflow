from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values


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


def get_podcast_list_from_db(**context) -> [list]:
    """Check database to get a list of podcasts that need to be checked for updates.
    Also get the last 'published_date' for each podcast.
    """
    conn = PostgresHook(postgres_conn_id="aws_podcastdb").get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as curs:
        curs.execute(
            """
            SELECT id, feed_url, bucket_directory
            FROM podcasts
            WHERE bucket_sync = True
            """
        )
        query_result = curs.fetchall()
        podcast_list = [dict(row) for row in query_result]
        if len(podcast_list) > 0:
            for podcast in range(len(podcast_list)):
                podcast_id = podcast_list[podcast]["id"]
                curs.execute(
                    f"""
                    SELECT published_date
                    FROM episodes
                    WHERE podcast_id = {podcast_id}
                    ORDER BY published_date DESC
                    LIMIT 1
                    """
                )
                latest_episode = curs.fetchone()
                if latest_episode:
                    published_date = latest_episode["published_date"].strftime("%Y-%m-%d %H:%M:%S%z")
                    podcast_list[podcast]["last_updated"] = published_date
                else:
                    podcast_list[podcast]["last_updated"] = None
    context["ti"].xcom_push(key="podcast_list", value=podcast_list)


def add_new_episodes_to_db(**context):
    podcast_list = context["ti"].xcom_pull(task_ids="get_podcast_list_from_db", key="podcast_list")
    print(podcast_list)


with DAG(
    dag_id="podcast_scrape",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:
    get_podcast_list_from_db = PythonOperator(
        task_id="get_podcast_list_from_db",
        python_callable=get_podcast_list_from_db,
        provide_context=True
    )
    add_new_episodes_to_db = PythonOperator(
        task_id="add_new_episodes_to_db",
        python_callable=add_new_episodes_to_db,
        provide_context=True
    )
    # should 'get_episode_list' add new episdoes to db? Or should that be separate task?
    download_episodes_to_s3 = DummyOperator(
        task_id="download_episodes_to_s3"
    )

    get_podcast_list_from_db >> add_new_episodes_to_db >> download_episodes_to_s3
