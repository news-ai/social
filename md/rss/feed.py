# -*- coding: utf-8 -*-
# Third-party app imports
import feedparser


def feed_url_to_articles(feed_url):
    articles = feedparser.parse(feed_url)
    return articles['entries']
