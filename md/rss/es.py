# -*- coding: utf-8 -*-
# Stdlib imports
import json
import os

# Third-party app imports
import certifi
from elasticsearch import Elasticsearch, helpers

# Elasticsearch
ELASTICSEARCH_USER = os.environ['NEWSAI_ELASTICSEARCH_USER']
ELASTICSEARCH_PASSWORD = os.environ['NEWSAI_ELASTICSEARCH_PASSWORD']

# Elasticsearch setup
es = Elasticsearch(
    ['https://search.newsai.org'],
    http_auth=(ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD),
    port=443,
    use_ssl=True,
    verify_certs=True,
    ca_certs=certifi.where(),
)


def get_rss_feeds_to_fetch():
    date_from = moment.now().locale("US/Eastern").timezone("Europe/London").subtract(days=1).replace(
        hours=0, minutes=0, seconds=0).format('YYYY-MM-DDTHH:mm:ss')

    query = {
        'size': 5000,
        'from': 0
    }

    res = es.search(index='md', doc_type='feeds', body=query)
    return res
