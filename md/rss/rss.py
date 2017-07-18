# -*- coding: utf-8 -*-
# Third-party app imports
from newspaper import Article

# Local app imports
import es


def process_rss_feed():
    pass


def get_rss_feeds_to_process_from_es():
    print es.get_rss_feeds_to_fetch()

get_rss_feeds_to_process_from_es()
