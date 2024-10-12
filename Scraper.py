#!/usr/bin/env python
# coding: utf-8

# In[1]:


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


# In[2]:


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
    client = Client('en-US')
    await client.login(
        auth_info_1=USERNAME,
        auth_info_2=EMAIL,
        password=PASSWORD
    )
    # client.save_cookies('cookies.json')
    client.load_cookies('cookies.json')
    return client


# In[3]:


from datetime import datetime, timedelta

def previous_day(date_str):
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    previous_date_obj = date_obj - timedelta(days=1)
    previous_date_str = previous_date_obj.strftime("%Y-%m-%d")
    return previous_date_str


# In[4]:


async def fetch_tweets(keyword, date):
    tweets_to_store = []
    tag = tags_db.find_one({"name": keyword})
    if tag:
        for hour in range(1, 25, 1):
            try:
                query = f"({tag['tag']}) until:{date}_{hour}:00:00_UTC since:{date}_{hour-1}:00:00_UTC lang:en"
                print(f"Getting tweets ({datetime.now()}) from {date} at {hour}")
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


# In[12]:


async def select_tag_and_date():
    pipeline = [
        {
            "$addFields": {
                "dateAsDate": {"$toDate": "$date"}
            }
        },
        {
            "$sort": {"dateAsDate": 1}
        },
        {
            "$limit": 1
        }
    ]
    result = list(daily_tweets_db.aggregate(pipeline))
    tags = list(daily_tweets_db.find({'date': result[0]['date']}, { 'tag_id': 1, '_id': 0 }))
    if len(tags) == len(list(tags_db.find({}))):
        for tag in tags_db.find({}):
            await fetch_tweets(tag['name'], previous_day(result[0]['date']))
    else:
        tags_found = [obj['tag_id'] for obj in tags]
        tags_r = list(tags_db.find(
            {"_id": {"$nin": tags_found}} 
        ))
        for tag in tags_r:
            await fetch_tweets(tag['name'], result[0]['date'])


# In[5]:


async def main():
    client = await twitter_login()
    await select_tag_and_date(client)
# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())


# In[ ]:


# for tag in tags_db.find({}):
#     await fetch_tweets(tag['name'], "2024-10-06")
# # os.system("shutdown /s /t 1")

