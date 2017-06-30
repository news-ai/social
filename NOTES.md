### Twitter sync

With elasticsearch if the error is that the window is too large then run:

`curl -XPUT "https://search.newsai.org/tweets/_settings" -d '{ "index" : { "max_result_window" : 500000 } }'`

The memory doesn't increase exponentially if it's in the ~100,000s. If more then start using the scroll api.
