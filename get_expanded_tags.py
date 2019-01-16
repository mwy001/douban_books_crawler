#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import random
import datetime
import requests
import pymongo
from bs4 import BeautifulSoup as bs

###### Get the expanded tags from each book's detail view page

###### MongoDB connections ######
g_mongo_client = pymongo.MongoClient("mongodb://localhost:27017")
g_douban_tags_coll = g_mongo_client["douban"]["tags"]
g_douban_books_coll = g_mongo_client["douban"]["books"]

g_requests_headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"
}

def current_time_millis():
    delta = datetime.datetime.utcnow() - datetime.datetime.utcfromtimestamp(0)
    return int(delta.total_seconds() * 1000)

if __name__ == "__main__":
    for one_book in g_douban_books_coll.find().sort([("show_detail_time", pymongo.ASCENDING)]):
        url = one_book["url"]
        r = requests.get(url, headers=g_requests_headers)
        soup = bs(r.text, "html.parser")

        tags = soup.select("a.tag")
        tags_arr = []
        for one_tag in tags:
            tag_name = one_tag.text.strip()
            tag_url = one_tag["href"]
            query = {"name":tag_name}
            payload = {"name":tag_name, "path":tag_url, "create_time":current_time_millis(), "crawl_time":0}
            print(payload)
            if not g_douban_tags_coll.find_one(query):
                g_douban_tags_coll.insert_one(payload)
            tags_arr.append(tag_name)
        g_douban_books_coll.update({"url":url}, {"$set":{"show_detail_time":current_time_millis(), "tags":tags_arr}}, upsert=False)
        time.sleep(random.randint(20, 60))
