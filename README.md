# Amazon MWAA 
- Files in this repository are automatically synced to Amazon S3. This makes it simple to add new DAGs, plugins and update the requirements file.

---
## Pipelines
### Podcast Scraper
1. Check database for list of podcast feeds to update.
    - This was done with the intention of making it simple to add new podcast RSS feeds. Any RSS feeds added to the 'podcasts' table will be processed in the following steps.
2. Check each podcast feed for metadata updates (including audio file URLs). Save the metadata to S3.
3. Get the metadata from S3 and add it to the 'episodes' database table.
4. Get a list of URLs from the 'episodes' table for any episodes that have not been downloaded. Download them to an S3 bucket. The download directory is specified for each podcast when it is added to the 'podcasts' table.
#### Resources Used
Computing: Amazon MWAA
Database: AWS Aurora Serverles (Postgres)

---
### Twitter Scraper
1. Get list of queries from database.
    - This is mean to make it simple to expand the list of search queries. This can be done directly through SQL, or a webapp could be setup to make the process simpler.
2. Get scrape results for each query.
    - Prior to starting the scrape, the database is checked to calculate which days have already been processed.
    - The scrape jobs are divided by day (i.e. A scrape with a 10 day date range will be split into 10 'jobs'.). This will make it easier to update the DAG and incorporate parallel processing in the future.
    - Scraping jobs are outsourced to a [Lambda function I setup](https://github.com/aarongzmn/tweet-scraper).

#### Resources Used
Computing: Amazon MWAA
Database: AWS Aurora Serverles (Postgres)