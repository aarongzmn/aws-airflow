# Amazon MWAA 
- Files in this repository are automatically synced to Amazon S3. This makes it simple to add new DAGs, plugins and update the requirements file.

---
## Pipelines
### Podcast Scraper
1. Check database for list of podcast feeds to update.
    - Additional podcast feeds can be added to the database. All RSS feeds that are added to the 'podcasts' table will be processed by the following steps.
2. Check each podcast feed for metadata updates (including audio file URLs). Save the metadata to S3.
3. Get the metadata from S3 and add it to the 'episodes' database table.
    - This includes a function to extract host/cohost/guest names from the show notes. This likely wouldn't work for all podcast feeds.
4. Get a list of URLs from the 'episodes' table for any episodes that have not been downloaded. Download them to an S3 bucket and update the 'podcasts' table with the S3 URL to indicate the file has been downloaded.
    - The download directory is specified for each podcast when it is added to the 'podcasts' table.
#### Resources Used
- Computing: Amazon MWAA
- Database: AWS Aurora Serverles (Postgres)

---
### Twitter Scraper
1. Get list of queries from database.
    - New queries can be added to the 'queries' table. This can be done directly through SQL, or a webapp (or Google Sheets, etc) could be created to make the process simpler.
2. Get scrape results for each query.
    - Prior to starting the scrape, the database is checked to calculate which days have already been processed.
    - The scrape jobs are divided by day (i.e. A scrape with a 10 day date range will be split into 10 'jobs'.). This will make it easier to update the DAG and incorporate parallel processing in the future.
    - **Scraping jobs are handed off** to a [Lambda function I setup](https://github.com/aarongzmn/tweet-scraper).

#### Resources Used
- Computing: Amazon MWAA, Lambda
- Database: AWS Aurora Serverles (Postgres)
