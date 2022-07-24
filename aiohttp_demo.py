# pip install aiohttp[speedups]
import pandas as pd
import aiohttp
import asyncio
import platform
from datetime import timedelta


if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def post_request(session, url, data):
    async with session.post(url, json=data) as resp:
        response = await resp.json()
        return response


async def main(url, query_list):
    async with aiohttp.ClientSession() as session:

        tasks = []
        for query in query_list:
            tasks.append(asyncio.ensure_future(post_request(session, url, query)))

        scrape_results = []
        response_list = await asyncio.gather(*tasks)
        for response in response_list:
            scrape_results.append(response["data"])
        return scrape_results


def query_factory(query_template, date_list):
    query_list = []
    for date in date_list:
        start_date = date.strftime("%Y-%m-%d")
        end_date = (date + timedelta(days=1)).strftime("%Y-%m-%d")
        query = query_template.replace("$STARTDATE", start_date).replace("$ENDDATE", end_date)
        query_list.append(
            {
                "start_date": start_date,
                "query": query
            }
        )
    return query_list


query_template = "Example Query start=$STARTDATE end=$ENDDATE"
date_list = pd.date_range(start="2022-01-01", end="2022-06-01")

query_list = query_factory(query_template, date_list)
url = "https://httpbin.org/post"
scrape_results = asyncio.run(main(url, query_list))
print(scrape_results)


def chunk_list(chunk_size, whole_list) -> list:
    chunks = [
        whole_list[i:i + chunk_size]
        for i in range(0, len(whole_list), chunk_size)
    ]
    return chunks
