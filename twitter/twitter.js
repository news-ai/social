'use strict';

var Q = require('q');
var elasticsearch = require('elasticsearch');
var moment = require('moment');
var gcloud = require('google-cloud')({
    projectId: 'newsai-1166'
});
var pubsub = gcloud.pubsub();

// Instantiate a elasticsearch client
var elasticSearchClient = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search1.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

var twitter = exports;

function getTweetPageFromEsLastWeek(offset) {
    var deferred = Q.defer();

    var dateTo = moment().format('YYYY-MM-DD');
    var dateFrom = moment().subtract(7, 'd')
    var lastWeek = dateFrom.format('YYYY-MM-DDTHH:mm:ss');

    elasticSearchClient.search({
        index: 'tweets',
        type: 'tweet',
        body: {
            "query": {
                "filtered": {
                    "query": {
                        "match_all": {}
                    },
                    "filter": {
                        "range": {
                            "data.CreatedAt": {
                                "gte": lastWeek
                            }
                        }
                    }
                }
            },
            "sort": [{
                "data.CreatedAt": {
                    "order": "desc",
                    "mode": "avg"
                }
            }],
            "size": 100,
            "from": 100 * offset,
        }
    }).then(function(resp) {
        var hits = resp.hits.hits;
        deferred.resolve(hits);
    }, function(err) {
        console.trace(err.message);
    });

    return deferred.promise;
}

function getTweetsFromEsLastWeek(offset, allData) {
    var deferred = Q.defer();

    getTweetPageFromEsLastWeek(offset).then(function(data) {
        if (data.length === 0) {
            deferred.resolve(allData);
        } else {
            var newData = allData.concat(data);
            deferred.resolve(getTweetsFromEsLastWeek(offset + 1, newData));
        }
    });

    return deferred.promise;
}

twitter.getTweetsFromEsLastWeek = getTweetsFromEsLastWeek;