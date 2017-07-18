# -*- coding: utf-8 -*-
# Third-party app imports
from newspaper import Article


def process_article(single_article):
    link = single_article['link']
    article = Article(link)
    article.download()
    article.parse()
    print article.authors
