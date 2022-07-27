from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta, timezone
from dateutil import parser
# import copy
import json
import time
import requests
from requests.exceptions import HTTPError
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)


default_args = {
    "owner": "Aaron Guzman",
    "depend_on_past": False,
    "start_date": datetime(2022, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(seconds=30),
}


class ParseTweet():
    """Use this to parse the tweet scrape object from the 'snscrape' library.
        - This will rename the dictionary keys so they are compatible with the database tables.
        - These lists will be used to update the 'tweets' and 'users' tablees.
    """
    def __init__(self, tweet, query_id, snapshot_date):
        self.tweet_dict = tweet
        self.query_id = query_id
        self.snapshot_date = snapshot_date

    def rename_keys_for_tweet_data(self):
        """Rename keys to match 'twitterdb' 'tweets' table
        """
        self.tweet_dict["url"] = self.tweet_dict.pop('url', None)
        self.tweet_dict["tweet_date"] = self.tweet_dict.pop('date', None)
        self.tweet_dict["tweet_content"] = self.tweet_dict.pop('content', None)
        self.tweet_dict["tweet_rendered_content"] = self.tweet_dict.pop('renderedContent', None)
        self.tweet_dict["id"] = self.tweet_dict.pop('id', None)
        self.tweet_dict["user_id"] = self.tweet_dict.pop('user', None)
        self.tweet_dict["reply_count"] = self.tweet_dict.pop('replyCount', None)
        self.tweet_dict["retweet_count"] = self.tweet_dict.pop('retweetCount', None)
        self.tweet_dict["like_count"] = self.tweet_dict.pop('likeCount', None)
        self.tweet_dict["quote_count"] = self.tweet_dict.pop('quoteCount', None)
        self.tweet_dict["conversation_id"] = self.tweet_dict.pop('conversationId', None)
        self.tweet_dict["lang"] = self.tweet_dict.pop('lang', None)
        self.tweet_dict["tweet_source"] = self.tweet_dict.pop('source', None)
        self.tweet_dict["source_url"] = self.tweet_dict.pop('sourceUrl', None)
        self.tweet_dict["source_label"] = self.tweet_dict.pop('sourceLabel', None)
        self.tweet_dict["outlinks"] = self.tweet_dict.pop('outlinks', None)
        self.tweet_dict["tco_outlinks"] = self.tweet_dict.pop('tcooutlinks', None)
        self.tweet_dict["media"] = self.tweet_dict.pop('media', None)
        self.tweet_dict["retweeted_tweet"] = self.tweet_dict.pop('retweetedTweet', None)
        self.tweet_dict["quoted_tweet"] = self.tweet_dict.pop('quotedTweet', None)
        self.tweet_dict["in_reply_to_tweet_id"] = self.tweet_dict.pop('inReplyToTweetId', None)
        self.tweet_dict["in_reply_to_user"] = self.tweet_dict.pop('inReplyToUser', None)
        self.tweet_dict["mentioned_users"] = self.tweet_dict.pop('mentionedUsers', None)
        self.tweet_dict["coordinates"] = self.tweet_dict.pop('coordinates', None)
        self.tweet_dict["place"] = self.tweet_dict.pop('place', None)
        self.tweet_dict["hashtags"] = self.tweet_dict.pop('hashtags', None)
        self.tweet_dict["cashtags"] = self.tweet_dict.pop('cashtags', None)
        self.tweet_dict["query_id"] = self.query_id
        return self.tweet_dict

    def rename_keys_for_user_data(self, users_list):
        """Rename keys to match 'twitterdb' 'users' table
        These data for these keys are all sourced from snscrape library scrape data.
        """
        users_list["username"] = users_list.pop("username")
        users_list["id"] = users_list.pop("id")
        users_list["display_name"] = users_list.pop("displayname")
        users_list["description"] = users_list.pop("description")
        users_list["raw_description"] = users_list.pop("rawDescription")
        users_list["description_urls"] = users_list.pop("descriptionUrls")
        users_list["verified"] = users_list.pop("verified")
        users_list["created"] = users_list.pop("created")
        users_list["followers_count"] = users_list.pop("followersCount")
        users_list["friends_count"] = users_list.pop("friendsCount")
        users_list["statuses_count"] = users_list.pop("statusesCount")
        users_list["favourites_count"] = users_list.pop("favouritesCount")
        users_list["listed_count"] = users_list.pop("listedCount")
        users_list["media_count"] = users_list.pop("mediaCount")
        users_list["location"] = users_list.pop("location")
        users_list["protected"] = users_list.pop("protected")
        users_list["link_url"] = users_list.pop("linkUrl")
        users_list["link_t_courl"] = users_list.pop("linkTcourl")
        users_list["profile_image_url"] = users_list.pop("profileImageUrl")
        users_list["profile_banner_url"] = users_list.pop("profileBannerUrl")
        users_list["label"] = users_list.pop("label")
        users_list["user_type"] = users_list.pop("user_type")
        users_list["snapshot_date"] = users_list.pop("snapshot_date")
        return users_list

    def split_tweet_data_and_user_data(self) -> (dict, dict):
        tweet_dict = self.rename_keys_for_tweet_data()
        users_list = []

        user_id = tweet_dict.pop("user_id")
        if user_id:
            user_id["user_type"] = "author"
            users_list.append(user_id)
            tweet_dict["user_id"] = user_id["id"]
        else:
            tweet_dict["user_id"] = None

        reply_to = tweet_dict.pop("in_reply_to_user")
        if reply_to:
            reply_to["user_type"] = "reply_to"
            users_list.append(reply_to)
            tweet_dict["in_reply_to_user"] = reply_to["id"]
        else:
            tweet_dict["in_reply_to_user"] = None

        mentioned_list = tweet_dict.pop("mentioned_users")
        if mentioned_list:
            # for loop to remove duplicates for mentioned list
            mentioned_user_ids = []
            for m in mentioned_list:
                if m["id"] not in mentioned_user_ids:
                    users_list.append(m)
                    mentioned_user_ids.append(m["id"])
            tweet_dict["mentioned_users"] = mentioned_user_ids
        else:
            tweet_dict["mentioned_users"] = None
        # Add these keys/values to all items in 'users_list'
        for user in users_list:
            user["tweet_id"] = tweet_dict["id"]
            user["query_id"] = self.query_id
            user["snapshot_date"] = self.snapshot_date
            self.rename_keys_for_user_data(user)
        return tweet_dict, users_list


# HELPER FUNCTION
def select_from_database(sql) -> [dict]:
    """SELECT database and return results in dictionary format.
    """
    conn = PostgresHook(postgres_conn_id="aws_twitterdb").get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as curs:
        curs.execute(f"{sql}")
        query_result = curs.fetchall()
        result_list = [dict(row) for row in query_result]
    return result_list

# HELPER FUNCTION
def batch_insert_into_database(table_name: str, record_list: [dict]) -> list:
    """BULK INSERT INTO database (1000 rows at a time). Input list shoud be items in dictionary format.
    """
    col_names = ", ".join(record_list[0].keys())
    insert_values = [tuple(e.values()) for e in record_list]
    with PostgresHook(postgres_conn_id="aws_twitterdb").get_conn() as conn:
        with conn.cursor() as curs:
            sql = f"INSERT INTO {table_name} ({col_names}) VALUES %s"
            psycopg2.extras.execute_values(curs, sql, insert_values, page_size=1000)
    return


# TASK 1
def get_query_tasks_from_database(**context) -> [dict]:
    """
    1. Get list of active queries and start/end dates for each query.
    2. Get list of days that have scrape data in database table for each query.
    3. Compare lists 1 and 2 to figure out which remaining days need scrape data.
    """
    sql = "SELECT * FROM queries WHERE active is true"
    query_tasks = select_from_database(sql)
    print(f"Active queries found: {len(query_tasks)}")
    sql = """
    SELECT query_id, CAST(tweet_date as DATE), COUNT(*)
    FROM tweets
    GROUP BY query_id, CAST(tweet_date as DATE)
    ORDER BY tweet_date ASC
    """
    days_scraped = select_from_database(sql)

    max_scrape_date = datetime.now(timezone.utc) - timedelta(days=2)  # limit scrape to data > 1 day old
    for i in range(len(query_tasks)):
        db_scrape_data = [q["tweet_date"].strftime("%Y-%m-%d") for q in days_scraped if q["query_id"] == query_tasks[i]["id"]]

        scrape_from = query_tasks[i]["scrape_from_date"]
        scrape_to = min(query_tasks[i]["scrape_to_date"], max_scrape_date)
        date_range_dt = pd.date_range(start=scrape_from, end=scrape_to)
        date_range_str = list(date_range_dt.strftime("%Y-%m-%d"))

        query_tasks[i]["to_scrape"] = [date for date in date_range_str if date not in db_scrape_data]
        del query_tasks[i]["scrape_from_date"]
        del query_tasks[i]["scrape_to_date"]
    context["ti"].xcom_push(key="database_query_tasks", value=query_tasks)

# TASK 2
def get_twitter_data_and_save_to_s3(**context):
    """Get scrape results for each day for each query.
    The scraping job is offloaded to a Lambda function that returns a list of scrape results.
    Lastly, the scrape results are INSERTED INTO the database 'tweets' and 'users' tables.
    """
    query_tasks = context["ti"].xcom_pull(task_ids="get_query_tasks_from_database", key="database_query_tasks")
    s3_file_list = []
    for query in query_tasks:
        query_id = query["id"]
        date_list = query["to_scrape"]
        if len(date_list) == 0:
            print(f"Scrape for query ID {query_id} is up to date. Moving onto next query in list.")
        else:
            print(f"Scraping {len(date_list)} days worth of updates for query ID: {query_id}: {date_list}")
            for start_date in date_list:
                start_date_dt = datetime.strptime(start_date, '%Y-%m-%d')
                end_date_dt = (start_date_dt + timedelta(days=1)).date()
                end_date = end_date_dt.strftime('%Y-%m-%d')
                try:
                    query_string = query["query_string"].replace("$STARTDATE", start_date).replace("$ENDDATE", end_date)
                    bucket_name = "twitter-scrape-results"  # convert to environment variable
                    key_name = f"query_id_{query_id}_starting_{start_date}_ending{end_date}.json"
                    data = {
                        "query_string": query_string,
                        "bucket_name": bucket_name,
                        "key_name": key_name
                    }
                    # convert lambda URL to environment variable
                    r = requests.post("https://nzq2m6h2wfkhmzvvjtj4ce35ve0pnixf.lambda-url.us-west-2.on.aws/", json=data)
                    r.raise_for_status()
                    print(r.text)
                    data["query_id"] = query_id
                    data["snapshot_date"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")
                    s3_file_list.append(data)

                except HTTPError as e:
                    print(f"Request failed with status code {e.response.status_code} while working on date {start_date} for query: {query_string}")
                    time.sleep(15)
                    continue
    context["ti"].xcom_push(key="s3_file_list", value=s3_file_list)

# TASK 3
def load_s3_twitter_data_into_database(**context) -> None:
    """Get each S3 file in xcom list and insert it into the database.
    TODO: It may be a good idea to combine the S3 files so the database INSERT INTO is more efficient.
    """
    s3_file_list = context["ti"].xcom_pull(task_ids="get_twitter_data_and_save_to_s3", key="s3_file_list")
    s3_hook = S3Hook(aws_conn_id='aws_default')
    print(f"Loading {len(s3_file_list)} S3 file(s) into database.")
    for s3_file in s3_file_list:
        query_string = s3_file["query_string"]
        key_name = s3_file["key_name"]
        bucket_name = s3_file["bucket_name"]
        query_id = s3_file["query_id"]
        snapshot_date = s3_file["snapshot_date"]
        response_str = s3_hook.read_key(key_name, bucket_name)
        response_dict = json.loads(response_str)

        tweets_table_updates = []
        users_table_updates = []
        for tweet in response_dict:
            tweets_dict, users_dict = ParseTweet(tweet, query_id, snapshot_date).split_tweet_data_and_user_data()
            tweets_table_updates.append(tweets_dict)
            users_table_updates.extend(users_dict)

        batch_insert_into_database("tweets2", tweets_table_updates)
        batch_insert_into_database("users2", users_table_updates)
        print(f"Finished inserting new records for query: {query_string}")


with DAG(
    dag_id="twitter_scrape",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    get_query_tasks_from_database = PythonOperator(
        task_id="get_query_tasks_from_database",
        python_callable=get_query_tasks_from_database,
        provide_context=True,
    )
    
    get_twitter_data_and_save_to_s3 = PythonOperator(
        task_id="get_twitter_data_and_save_to_s3",
        python_callable=get_twitter_data_and_save_to_s3,
        provide_context=True,
    )

    load_s3_twitter_data_into_database = PythonOperator(
        task_id="load_s3_twitter_data_into_database",
        python_callable=load_s3_twitter_data_into_database,
        provide_context=True,
    )
    
    get_query_tasks_from_database >> get_twitter_data_and_save_to_s3 >> load_s3_twitter_data_into_database
