# -*- coding: utf-8 -*-
# Stdlib imports
import json
import os

# Third-party app imports
import certifi
import moment
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


def add_headline_to_md():
    pass


def get_rss_feeds_to_fetch():
    updated_before = moment.now().locale(
        "US/Eastern").subtract(minutes=15).format('YYYY-MM-DDTHH:mm:ss')

    query = {
        'size': 5000,
        'from': 0,
        'query': {
            'bool': {
                'must': [{
                    'range': {
                        'data.Updated': {
                            'to': updated_before
                        }
                    }
                }]
            }
        }
    }

    res = es.search(index='md', doc_type='feeds', body=query)
    return res
