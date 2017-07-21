# -*- coding: utf-8 -*-
# Third-party app imports
from newspaper import Article


def process_article(single_article):
    # Process article using Newspaper
    article = Article(single_article['link'])
    article.download()
    article.parse()
    article.nlp()

    # Create profile of article that will
    # be added to elasticsearch
    article_details = {
        '_id': single_article['link'],
        'url': single_article['link'],

        'title': article.title,

        'top_image': article.top_image,

        'keywords': article.keywords,
        'summary': article.summary,
    }

    print article_details
