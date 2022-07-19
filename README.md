# Amazon MWAA 
This repository is synced to the S3 bucket linked in MWAA.

## Pipelines
### Podcast Scraper
1. Check database for list of podcast feeds to update.
2. Check each podcast feed for metadata updates (including audio file URLs). Save the metadata to S3.
3. Get the metadata from S3 and add it to the database.
4. Get a list of URLs to the audio files from the 'episodes' database table and download the files to S3.
#### Resources Used
Computing: Amazon MWAA
Database: AWS Aurora Serverles (Postgres)

### Twitter Scraper
1. Get list of queries from database.
    - The decision to output a list here 
2. Get scrape results for each query.
    - Prior to starting the scrape, the database is checked to calculate which days have already been processed.
    - The scrape jobs are divided by days. This will make it easier to update the DAG and incorporate parallel processing in the future.
    - Scraping jobs are outsourced to a [Lambda function I setup](https://github.com/aarongzmn/tweet-scraper).
