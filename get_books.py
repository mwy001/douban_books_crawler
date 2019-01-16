#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import random
import datetime
import requests
import pymongo
from bs4 import BeautifulSoup as bs

###### Get the books info

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
    for one_tag in g_douban_tags_coll.find().sort([("crawl_time", pymongo.ASCENDING)]).limit(100):
        tag_name = one_tag["name"]
        tag_path = one_tag["path"]

        pos = 0
        finished = False

        while finished is False:
            tag_books_url = "https://book.douban.com%s?start=%d&type=T" % (tag_path, pos)
            r = requests.get(tag_books_url, headers=g_requests_headers)
            if r.text.find("没有找到符合条件的图书") != -1:
                finished = True
            else:
                soup = bs(r.text, "html.parser")
                books = soup.select("li.subject-item")
                for one_book in books:
                    title = one_book.select_one("a[title]")["title"]
                    detail_page_url = one_book.select_one("a[title]")["href"]
                    cover_img = one_book.select_one("div.pic").select_one("img[src]")["src"]
                    pub_info = one_book.select_one("div.pub").text.strip()

                    rating_score = ""
                    rating_score_tag = one_book.select_one("div.star").select_one("span.rating_nums")
                    if rating_score_tag:
                        rating_score = rating_score_tag.text.strip()
                    
                    rating_count = ""
                    rating_count_tag = one_book.select_one("div.star").select_one("span.pl")
                    if rating_count_tag:
                        rating_count = rating_count_tag.text.strip()

                    query = {"url":detail_page_url}
                    payload = {
                        "title":title,
                        "cover_image":cover_img,
                        "url":detail_page_url,
                        "publication_info":pub_info,
                        "rating_score":rating_score,
                        "rating_count":rating_count,
                        "create_time":current_time_millis(),
                        #"show_detail_time":0
                    }

                    print(payload)
                    g_douban_books_coll.update(query, payload, upsert=True)

                    pos = pos + 1
            time.sleep(random.randint(10, 20))

        g_douban_tags_coll.update({"path":tag_path}, {"$set":{"crawl_time":current_time_millis()}}, upsert=False)
