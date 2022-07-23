from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
import feedparser
import requests
import psycopg2
import logging
from psycopg2.extras import RealDictCursor, execute_values
from dateutil import parser
from datetime import datetime, timedelta, timezone
import json
import re
import time


psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)


default_args = {
    "owner": "Aaron Guzman",
    "depend_on_past": False,
    "start_date": datetime(2020, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(seconds=30),
}

# TASK 1
def get_podcast_list_from_db(**context) -> [list]:
    """Check database to get a list of podcasts that need to be checked for updates.
    Also get the last 'published_date' for each podcast.
    """
    conn = PostgresHook(postgres_conn_id="aws_podcastdb").get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as curs:
        curs.execute(
            """
            SELECT *
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
    logging.info(f"Found {len(podcast_list)} podcasts to update.")
    context["ti"].xcom_push(key="podcast_list", value=podcast_list)

# TASK 2 Helper Function
def get_feed_data_for_new_episodes(podcast_data):
    """Get updated metadata for each episode in each podcast feed and save the metadata to an S3 bucket.
    """
    feed_url = podcast_data["feed_url"]
    last_updated = podcast_data["last_updated"]
    if last_updated is None:
        last_updated = datetime(year=2000, month=1, day=1).replace(tzinfo=timezone.utc)
    else:
        last_updated = parser.parse(podcast_data["last_updated"])
        logging.info(f"Using cutoff date: {last_updated}")
    page = 0  # set to 1+ to emulate existing table data
    if page != 0:
        logging.info(f"Starting scrape at page {page}")
    next_page = True
    cutoff_reached = False
    podcast_feed_data = []
    while next_page is True and cutoff_reached is False:
        page += 1
        paginated_feed_url = feed_url + f"?paged={page}"
        feed_data = feedparser.parse(paginated_feed_url)
        episode_list = feed_data["entries"]
        if len(episode_list) == 0:
            next_page = False
        else:
            for episode in episode_list:
                publish_date = parser.parse(episode["published"])
                if publish_date > last_updated:
                    episode_dict = json.loads(json.dumps(episode, default=lambda o: getattr(o, '__dict__', str(o))))
                    del episode_dict["published_parsed"]
                    podcast_feed_data.append(episode_dict)
                else:
                    cutoff_reached = True
            time.sleep(2)  # avoid overloading server
    return podcast_feed_data

# TASK 2 Helper Function
def regex_expisode_cast(pattern: str, string: str) -> list:
    """Use regular expression to search for pattern in text.
    This is used to extract host/cohost/guest names from the show notes.
    Note: This likely won't work for new podcats that are added.
    Args:
        pattern (str): Regex pattern
        string (str): String that will be checked against pattern
    Returns:
        str: String is returned if results are found. Returns
    """
    try:
        resplit = re.split(pattern, string)
        trim_text = resplit[1].split(">")[1].split("<")[0]
        clean_text = trim_text.strip().replace("&#8220;", '"').replace("&#8221;", '"')
        while clean_text[0] == ":":
            clean_text = clean_text[1:]
        cast_list = re.split(" and |, ", clean_text.strip())
    except:
        return None
    return cast_list

# TASK 2 Helper Function
def get_key_attributes_from_feed_data(podcast_feed_data, podcast_id, get_cast=True) -> [dict]:
    """
    Args:
        podcast_feed_data (list): 
        podcast_id (int): _description_
        get_cast (bool): Flag used to parse cohost results from a specific podcast feed.
            If this is going to be a generalized podcast scraper, it should probably
            be removed from this function because it likely won't work on other feeds.

    Returns:
        [dict]: List of dictionaries containing relevant data to be saved in database.
    """

    key_episode_data = []
    for episode in podcast_feed_data:
        episode_data = {
            "podcast_id": podcast_id,
            "title": episode.get("title"),
            "website_page": episode.get("id"),
            "file_save_location": None,
            "author": episode.get("author"),
            "authors": episode.get("authors"),
            "summary": episode.get("summary"),
        }
        site_url_filter = [x for x in episode["links"] if x["type"] == "text/html"]
        if len(site_url_filter) > 0:
            episode_data["website_url"] = site_url_filter[0]["href"]
        else:
            episode_data["website_url"] = None

        file_url_filter = [x for x in episode["links"] if x["type"] == "audio/mpeg"]
        if len(file_url_filter) > 0:
            episode_data["file_source"] = file_url_filter[0]["href"]
        else:
            episode_data["file_source"] = None

        if episode.get("published"):
            parsed_dt = parser.parse(episode["published"])
            published_date = parsed_dt.strftime("%Y-%m-%d %H:%M:%S%z")
            episode_data["published_date"] = published_date
        else:
            episode_data["published_date"] = None

        if episode.get("tags"):
            episode_data["tags"] = [i["term"] for i in episode["tags"]]
        else:
            episode_data["tags"] = None

        if episode.get("summary_detail"):
            episode_data["summary_text"] = episode["summary_detail"].get("value")
        else:
            episode_data["summary_text"] = None

        content_html_filter = [x for x in episode["content"] if x["type"] == "text/html"]
        if len(content_html_filter) > 0:
            content_html = content_html_filter[0].get("value")
        else:
            content_html = None
        episode_data["content_html"] = content_html

        if get_cast:
            if content_html and "Host" in content_html:
                episode_data["hosts"] = regex_expisode_cast("Host|Hosts|Hosted by", content_html)
            else:
                episode_data["hosts"] = None
            if content_html and "Cohost" in content_html:
                episode_data["cohosts"] = regex_expisode_cast("Cohost|Cohosts", content_html)
            else:
                episode_data["cohosts"] = None
            if content_html and "Guest" in content_html:
                episode_data["guests"] = regex_expisode_cast("Guest|Guests", content_html)
            else:
                episode_data["guests"] = None
        key_episode_data.append(episode_data)
    return key_episode_data

# TASK 2
def get_new_episodes_and_save_to_s3(**context) -> [dict]:
    """Get the metadata from S3 for each podcast and use that to update the 'episodes' database table.
    """
    podcast_list = context["ti"].xcom_pull(task_ids="get_podcast_list_from_db", key="podcast_list")

    for podcast in range(len(podcast_list)):
        bucket_name = podcast_list[podcast]["bucket_name"]
        metadata_directory = podcast_list[podcast]["metadata_directory"]
        key_name = f"{datetime.now().strftime('%Y-%m-%dT%H-%M-%S')}.json"

        podcast_feed_data = get_feed_data_for_new_episodes(podcast_list[podcast])
        string_data = json.dumps(podcast_feed_data)
        key_directory = f"{metadata_directory}/source/{key_name}"
        podcast_list[podcast]["source_directory"] = key_directory
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_hook.load_string(
            string_data,
            key=key_directory,
            bucket_name=bucket_name,
            replace=True
        )

        podcast_id = podcast_list[podcast]["id"]
        key_episode_data = get_key_attributes_from_feed_data(podcast_feed_data, podcast_id, get_cast=True)
        logging.info(f"Found {len(key_episode_data)} episodes to add to database.")
        string_data = json.dumps(key_episode_data)
        key_directory = f"{metadata_directory}/processed/{key_name}"
        podcast_list[podcast]["processed_directory"] = key_directory
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_hook.load_string(
            string_data,
            key=key_directory,
            bucket_name=bucket_name,
            replace=True
        )
    context["ti"].xcom_push(key="podcast_feed_updates", value=podcast_list)


def batch_insert_into_database(table_name: str, episodes: [dict]) -> list:
    """Takes a list of dictionary items and adds the items (in bulk) to the given database table.
    The dictionary keys must match the column names in the datbase table.
    """
    col_names = ", ".join(episodes[0].keys())
    insert_values = [tuple(e.values()) for e in episodes]
    with PostgresHook(postgres_conn_id="aws_podcastdb").get_conn() as conn:
        with conn.cursor() as curs:
            sql = f"INSERT INTO {table_name} ({col_names}) VALUES %s RETURNING id"
            insert_result = psycopg2.extras.execute_values(curs, sql, insert_values, page_size=1000, fetch=True)
            logging.info(f"Added {len(insert_result)} podcast episodes to database.")
    return

# TASK 3
def add_new_episodes_to_db(**context):
    """Get podcast feed metadata from S3 directory and add it to the 'episodes' database table.
    """
    podcast_list = context["ti"].xcom_pull(task_ids="get_new_episodes_and_save_to_s3", key="podcast_feed_updates")
    for podcast in range(len(podcast_list)):
        key_directory = podcast_list[podcast]["processed_directory"]
        bucket_name = podcast_list[podcast]["bucket_name"]
        s3_hook = S3Hook(aws_conn_id='aws_default')
        response = s3_hook.read_key(key_directory, bucket_name)
        episode_list = json.loads(response)
        if len(episode_list) > 0:
            table_name = "episodes"
            batch_insert_into_database(table_name, episode_list)

# TASK 4
def download_episodes_to_s3():
    """Get list of episodes that have not been downloaded from the database.
    Download the episodes and update the database with the download location in S3.
    """
    s3_hook = S3Hook(aws_conn_id='aws_default')
    with PostgresHook(postgres_conn_id="aws_podcastdb").get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as curs:
            curs.execute(
                """
                SELECT episodes.id, episodes.file_source, episodes.published_date, podcasts.bucket_name, podcasts.audio_directory
                FROM episodes
                LEFT JOIN podcasts 
                ON episodes.podcast_id = podcasts.id
                WHERE episodes.file_source IS NOT NULL
                    AND episodes.file_save_location IS NULL
                LIMIT 100
                """
            )
            query_result = curs.fetchall()
            episode_list = [dict(row) for row in query_result]
    for episode in episode_list:
        episode_id = episode["id"]
        audio_directory = episode["audio_directory"]
        bucket_name = episode["bucket_name"]
        audio_file = requests.get(episode["file_source"]).content
        published_date = episode["published_date"].strftime('%Y-%m-%dT%H-%M-%S')
        orig_name = episode["file_source"].split("/")[-1]
        key_directory = f"{audio_directory}/{published_date} - {orig_name}"
        s3_hook.load_bytes(
            audio_file,
            key=key_directory,
            bucket_name=bucket_name,
            replace=True
        )
        logging.info(key_directory)
        with PostgresHook(postgres_conn_id="aws_podcastdb").get_conn() as conn:
            with conn.cursor() as curs:
                curs.execute(
                    f"""
                    UPDATE episodes
                    SET file_save_location = '{key_directory}'
                    WHERE id = {episode_id}
                    """
                )
    return


with DAG(
    dag_id="podcast_scrape",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:
    # Get list of podcast from 'podcasts' table that need to be checked for updates.
    get_podcast_list_from_db = PythonOperator(
        task_id="get_podcast_list_from_db",
        python_callable=get_podcast_list_from_db,
        provide_context=True
    )
    # Check each podcast feed for updates.
    # If there are udpates, parse the feed for new episode metadata and save data in S3.
    get_new_episodes_and_save_to_s3 = PythonOperator(
        task_id="get_new_episodes_and_save_to_s3",
        python_callable=get_new_episodes_and_save_to_s3,
        provide_context=True
    )
    # Get the S3 metadata and add it to the 'episodes' table.
    # This metadata will contain links to the audio files.
    add_new_episodes_to_db = PythonOperator(
        task_id="add_new_episodes_to_db",
        python_callable=add_new_episodes_to_db,
        provide_context=True
    )
    # Check 'episodes' table for any episodes that do not have a matching file in S3.
    # The 'episodes' table contains a column with a link to the source audio file and
    #     a destination column where a directory is stored once the audio file has been retrieved.
    #     If destination column is empty, that means the audio file hasn't been saved.
    download_episodes_to_s3 = PythonOperator(
        task_id="download_episodes_to_s3",
        python_callable=download_episodes_to_s3
    )
    get_podcast_list_from_db >> get_new_episodes_and_save_to_s3 >> add_new_episodes_to_db >> download_episodes_to_s3
