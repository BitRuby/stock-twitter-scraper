#!/usr/bin/env python
# coding: utf-8

# In[42]:


import pymongo
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
import os

uri = os.environ.get('MONGODB_URI', None)
client = MongoClient(uri, server_api=ServerApi('1'))

try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

# local_client = pymongo.MongoClient("mongodb://localhost:27017/")
# local_mydb = local_client["phd"]
# local_tags_db = local_mydb["x_com_tags"]
# local_tw_db = local_mydb["tweets"]
# local_daily_tweets_db = local_mydb["daily_tweets"]
  
mydb = client["phd"]
tags_db = mydb["x_com_tags"]
tw_db = mydb["tweets"]
daily_tweets_db = mydb["daily_tweets"]


# In[28]:


from twikit import Client, TooManyRequests, Unauthorized
import pandas as pd
import numpy as np
from datetime import datetime
import asyncio
import csv
import random
from random import randint
import time
import os

USERNAME = os.environ.get('TWITTER_USR', None)
EMAIL = os.environ.get('TWITTER_EMAIL', None)
PASSWORD = os.environ.get('TWITTER_PASS', None)

async def twitter_login():
    try: 
        client = Client('en-US')
        await client.login(
            auth_info_1=USERNAME,
            auth_info_2=EMAIL,
            password=PASSWORD
        )
        client.save_cookies('cookies.json')
        print("Logged in and saving cookies!")
    except Exception as e:
        print(e)
    finally:
        client.load_cookies('cookies.json')
        print("Logged in via cookies!")
        return client
    return client


# In[29]:


from datetime import datetime, timedelta

def next_day(date_str):
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    next_date_obj = date_obj + timedelta(days=1)
    next_date_str = next_date_obj.strftime("%Y-%m-%d")
    return next_date_str


# In[30]:


async def fetch_tweets(client, keyword, date):
    tweets_to_store = []
    tag = tags_db.find_one({"name": keyword})
    if tag:
        for hour in range(1, 25, 1):
            for minute in range(2):
                try:
                    hour_until = hour - 1 if minute == 0 else hour - 1 if hour == 24 else hour
                    minute_until = "30" if minute == 0 else "59" if hour == 24 else "00"
                    minute_since = "30" if minute == 1 else "00"
                    query = f"({tag['tag']}) until:{date}_{hour_until}:{minute_until}:00_UTC since:{date}_{hour-1}:{minute_since}:00_UTC lang:en"
                    print(f"Getting tweets ({datetime.now()}) from {date} from {date}_{hour-1}:{minute_since} to {hour_until}:{minute_until}")
                    tweets = await client.search_tweet(query, 'Latest')
                    for tweet in tweets:
                        tweets_to_store.append({
                            "name": tweet.user.name,
                            "likes": tweet.favorite_count, 
                            "content": tweet.text,
                            "created_at": tweet.created_at,
                            "tag_id": tag["_id"]
                        })
                    wait = randint(5,10)
                    time.sleep(wait)
                except TooManyRequests as e:
                    rate_limit_reset = datetime.fromtimestamp(e.rate_limit_reset)
                    print(f"Rate limit reached {datetime.now()}, wait until {rate_limit_reset}")
                    wait_time = rate_limit_reset - datetime.now()
                    time.sleep(wait_time.total_seconds())
        result = tw_db.insert_many(tweets_to_store)
        daily_tweets_db.insert_one({
            "date": date,
            "tweet_ids": result.inserted_ids,
            "tag_id": tag["_id"]
        })


# In[31]:


async def select_tag_and_date(client):
    pipeline = [
        {
            "$addFields": {
                "dateAsDate": {"$toDate": "$date"}
            }
        },
        {
            "$sort": {"dateAsDate": -1}
        },
        {
            "$limit": 1
        }
    ]
    result = list(daily_tweets_db.aggregate(pipeline))
    tags = list(daily_tweets_db.find({'date': result[0]['date']}, { 'tag_id': 1, '_id': 0 }))
    if len(tags) >= len(list(tags_db.find({}))):
        for tag in tags_db.find({}):
            await fetch_tweets(client, tag['name'], next_day(result[0]['date']))
    else:
        tags_found = [obj['tag_id'] for obj in tags]
        tags_r = list(tags_db.find(
            {"_id": {"$nin": tags_found}} 
        ))
        for tag in tags_r:
            await fetch_tweets(client, tag['name'], result[0]['date'])


# In[24]:


async def main():
    client = await twitter_login()
    await select_tag_and_date(client)
if __name__ == "__main__":
    asyncio.run(main())


# In[25]:


# for tag in tags_db.find({}):
#     await fetch_tweets(tag['name'], "2024-10-06")
# # os.system("shutdown /s /t 1")

