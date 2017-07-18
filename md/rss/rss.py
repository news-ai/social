# -*- coding: utf-8 -*-
# Third-party app imports
from newspaper import Article

# Local app imports
import es


def process_rss_feed(feed):
    print feed['_source']['data']['URL']


def get_rss_feeds_to_process_from_es():
    feeds = es.get_rss_feeds_to_fetch()
    for feed in feeds['hits']['hits']:
        if '_source' in feed and 'data' in feed['_source'] and 'URL' in feed['_source']['data']:
            process_rss_feed(feed)

get_rss_feeds_to_process_from_es()
