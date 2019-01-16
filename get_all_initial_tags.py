#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import requests
import pymongo
from bs4 import BeautifulSoup as bs

###### Get the initial tags from the "tags" page

g_all_tags_url = "https://book.douban.com/tag"

###### MongoDB connections ######
g_mongo_client = pymongo.MongoClient("mongodb://localhost:27017")
g_douban_tags_coll = g_mongo_client["douban"]["tags"]

g_requests_headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36"
}

def current_time_millis():
    delta = datetime.datetime.utcnow() - datetime.datetime.utcfromtimestamp(0)
    return int(delta.total_seconds() * 1000)

if __name__ == "__main__":
    r = requests.get(g_all_tags_url, headers=g_requests_headers)
    soup = bs(r.text, "html.parser")
    tag_tables = soup.select("table.tagCol")

    for one_tag_table in tag_tables:
        tags = one_tag_table.select("a")
        for t in tags:
            query = {"name":t.text}
            payload = {"name":t.text, "path":t.get("href"), "create_time":current_time_millis(), "crawl_time":0}
            if not g_douban_tags_coll.find_one(query):
                g_douban_tags_coll.insert_one(payload)
