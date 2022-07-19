from airflow import DAG
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta, timezone
from dateutil import parser
# import copy
import json

import requests
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)


default_args = {
    "owner": "Aaron Guzman",
    "depend_on_past": False,
    "start_date": datetime(2020, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(seconds=30),
}


class ParseTweet():
    """Use this to parse the tweet scrape object from the 'snscrape' library.
        - This will rename the dictionary keys so they are compatible with the database tables.
        - These lists will be used to update the 'tweets' and 'users' tablees.
    """
    def __init__(self, tweet, query_id):
        self.tweet_dict = tweet
        self.query_id = query_id

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

        # The Snowflake ID will be a unique ID that can be used to quickly tie the 'users' table to a specific tweet.
        # Because the 'users' table may have duplicates, this will help narrow down the specific entry with accurate information.
        timestamp = int(round(parser.parse(self.tweet_dict["tweet_date"]).timestamp()))
        self.tweet_dict["snowflake_id"] = str(self.tweet_dict["id"]) + str(timestamp)
        self.tweet_dict["query_id"] = self.query_id
        return self.tweet_dict

    def rename_keys_for_user_data(self, users_list):
        """Rename keys to match 'twitterdb' 'users' table
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
        users_list["snowflake_id"] = users_list.pop("snowflake_id")
        users_list["snapshot_date"] = users_list.pop("snapshot_date")
        return users_list

    def split_tweet_data_and_user_data(self) -> (dict, dict):
        tweet_dict = self.rename_keys_for_tweet_data()
        users_list = []
        user_id = tweet_dict.pop("user_id")
        if user_id:
            user_id["user_type"] = "owner"
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
            tweet_dict["mentioned_users"] = [i["id"] for i in mentioned_list]
            for mentioned in mentioned_list:
                mentioned["user_type"] = "mentioned"
                users_list.append(mentioned)
        else:
            tweet_dict["mentioned_users"] = None
        # Add these keys/values to all items in 'users_list'
        for user in users_list:
            user["snowflake_id"] = tweet_dict["snowflake_id"]
            user["snapshot_date"] = tweet_dict["tweet_date"]
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
            sql = f"INSERT INTO {table_name} ({col_names}) VALUES %s RETURNING id"
            insert_result = psycopg2.extras.execute_values(curs, sql, insert_values, page_size=1000, fetch=True)
    return insert_result


# TASK 1
def get_query_tasks_from_database(**context) -> [dict]:
    """
    1. Use the query start and end date range to creat a list of days for queries that need to be updated.
    2. Query the database to make a list of the days already have scrape data in the database.
    Compare lists 1 and 2 to figure out which remaining days need scrape data.
    """
    sql = "SELECT * FROM queries WHERE active is true"
    query_tasks = select_from_database(sql)
    for i in range(len(query_tasks)):
        query_tasks[i]["to_scrape"] = []
        # Create a list of days that have at least 1 tweet entry in the tweets table
        sql = f"""
            SELECT TWEETS.*
            FROM TWEETS
            JOIN
                (SELECT MIN(T2.TWEET_DATE) AS MIN_TIMESTAMP
                    FROM TWEETS T2
                    GROUP BY DATE(T2.TWEET_DATE)) T2 ON TWEETS.TWEET_DATE = T2.MIN_TIMESTAMP
            AND TWEETS.QUERY_ID = {query_tasks[i]["id"]}
            """
        response = select_from_database(sql)
        scrape_processed_days = [i["tweet_date"].strftime("%Y-%m-%d") for i in response]

        scrape_from = query_tasks[i]["scrape_from_date"]
        scrape_to = query_tasks[i]["scrape_to_date"]
        if scrape_to > datetime.now(timezone.utc):
            scrape_to = datetime.now(timezone.utc)
        date_list = pd.date_range(start=scrape_from, end=scrape_to)
        date_list = [i.strftime("%Y-%m-%d") for i in date_list]

        for date in date_list:
            if date not in scrape_processed_days:
                query_tasks[i]["to_scrape"].append(date)

        query_tasks[i]["scrape_from_date"] = scrape_from.strftime("%Y-%m-%d")
        query_tasks[i]["scrape_to_date"] = scrape_to.strftime("%Y-%m-%d")
    context["ti"].xcom_push(key="query_tasks", value=query_tasks)

# TASK 2
def scrape_tweets_for_query(**context):
    """Get scrape results for each day for each query.
    The scraping job is offloaded to a Lambda function that returns a list of scrape results.
    Lastly, the scrape results are INSERTED INTO the database 'tweets' and 'users' tables.
    """
    query_tasks = context["ti"].xcom_pull(task_ids="get_query_tasks_from_database", key="query_tasks")
    for query in query_tasks:
        date_list = query["to_scrape"]
        if len(date_list) == 0:
            print("No updates found at this time.")
        else:
            print(f"Scraping {len(date_list)} days worth of updates: {date_list}")
            query_template = query["query_string"]
            for query_date in date_list:
                start_date_dt = datetime.strptime(query_date, "%Y-%m-%d")
                start_date_str = start_date_dt.strftime('%Y-%m-%d')
                end_date_dt = (start_date_dt + timedelta(days=1)).date()
                end_date_str = end_date_dt.strftime('%Y-%m-%d')
                if end_date_dt >= datetime.utcnow().date():
                    # Only scrape if all of results are from the previous day
                    continue
                else:
                    print(start_date_dt, end_date_str)
                    query_string = query_template.replace("$STARTDATE", start_date_str).replace("$ENDDATE", end_date_str)
                    print(f"Getting tweets for query: {query_string}")
                    data = {"query": query_string}
                    # the lambda endpoing would be converted to an environment variable in production
                    r = requests.post("https://66s4jhi0ma.execute-api.us-west-2.amazonaws.com/api/query", json=data)
                    query_response = r.json()
                    print(f"Response contains {len(query_response)} tweets for query: {query_string}")
                    print(f"First item in response list is: {query_response[0]}")

                    tweets_table_updates = []
                    users_table_updates = []
                    query_id = query["id"]
                    for tweet in query_response:
                        tweets_dict, users_dict = ParseTweet(tweet, query_id).split_tweet_data_and_user_data()
                        tweets_table_updates.append(tweets_dict)
                        users_table_updates.extend(users_dict)

                    insert_result = batch_insert_into_database("tweets", tweets_table_updates)
                    print(f"Added {len(insert_result)} new tweets for query: {query_string}")
                    insert_result = batch_insert_into_database("users", users_table_updates)
                    print(f"Added {len(insert_result)} new users for query: {query_string}")
    return

with DAG(
    dag_id="twitter_scrape",
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=False,
) as dag:
    get_query_tasks_from_database = PythonOperator(
        task_id="get_query_tasks_from_database",
        python_callable=get_query_tasks_from_database,
        provide_context=True,
    )
    
    scrape_tweets_for_query = PythonOperator(
        task_id="scrape_tweets_for_query",
        python_callable=scrape_tweets_for_query,
        provide_context=True,
    )
    
    get_query_tasks_from_database >> scrape_tweets_for_query
