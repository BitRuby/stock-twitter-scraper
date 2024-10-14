#!/usr/bin/env python
# coding: utf-8

# In[10]:


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
enchanced_db = mydb["enchanced"]


# In[12]:


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

USERNAME = os.environ.get('TWITTER_USR_2', None)
EMAIL = os.environ.get('TWITTER_EMAIL_2', None)
PASSWORD = os.environ.get('TWITTER_PASS_2', None)

async def twitter_login():
    try: 
        client = Client('en-US')
        await client.login(
            auth_info_1=USERNAME,
            auth_info_2=EMAIL,
            password=PASSWORD
        )
        client.save_cookies('cookies_2.json')
        print("Logged in and saving cookies!")
    except Exception as e:
        print(e)
    finally:
        client.load_cookies('cookies_2.json')
        print("Logged in via cookies!")
        return client
    return client


# In[122]:


from datetime import datetime, timedelta

def prev_day(date_str):
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    prev_date_obj = date_obj - timedelta(days=1)
    prev_date_str = prev_date_obj.strftime("%Y-%m-%d")
    return prev_date_str


# In[144]:


async def fetch_tweets(client, keyword, date):
    tweets_to_store = []
    tag = tags_db.find_one({"name": keyword})
    if tag:
        for hour in range(0, 24, 1):
            try:
                query = f"({tag['tag']}) until:{date}_{hour}:30:00_UTC since:{date}_{hour}:00:00_UTC lang:en"
                print(f"Getting tweets ({datetime.now()}) from {date} at {hour}:30")
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
        enchanced_db.insert_one({
            "tag_id": tag["_id"],
            "date": date
        })
        daily_tweets_db.update_one(
            {"tag_id": tag["_id"], "date": date},
            {"$push": {"tweet_ids": {"$each": result.inserted_ids}}}
        )


# In[143]:


async def select_tag_and_date(client):
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
    result = list(enchanced_db.aggregate(pipeline))
    if result:
        tags = list(enchanced_db.find({'date': result[0]['date']}, { 'tag_id': 1, '_id': 0 }))
        if len(tags) == len(list(tags_db.find({"name": {"$ne": "Google"}}))):
            for tag in tags_db.find({"name": {"$ne": "Google"}}):
                await fetch_tweets(client, tag['name'], prev_day(result[0]['date']))
        else:
            tags_found = [obj['tag_id'] for obj in tags]
            tags_r = list(tags_db.find(
                {"name": {"$ne": "Google"}, "_id": {"$nin": tags_found}} 
            ))
            for tag in tags_r:
                await fetch_tweets(client, tag['name'], result[0]['date'])
    else:
        for tag in tags_db.find({"name": {"$ne": "Google"}}):
            await fetch_tweets(client, tag['name'], "2024-10-01")


# In[111]:


async def main():
    client = await twitter_login()
    await select_tag_and_date(client)
if __name__ == "__main__":
    asyncio.run(main())


# In[25]:


# for tag in tags_db.find({}):
#     await fetch_tweets(tag['name'], "2024-10-06")
# # os.system("shutdown /s /t 1")

