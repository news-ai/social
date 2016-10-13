/*!
 * Syncs single Instagram posts from Instagram API to ES
 */
'use strict';

var Q = require('q');
var request = require('requestretry');
var elasticsearch = require('elasticsearch');
var moment = require('moment');
var raven = require('raven');
var gcloud = require('google-cloud')({
    projectId: 'newsai-1166'
});

// Instantiate a elasticsearch client
var elasticSearchClient = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search1.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

// Instantiate a sentry client
var sentryClient = new raven.Client('https://666f957c7dd64957996c1b05675a960a:b942eb7df51d4f8780f55b7d4592a39f@sentry.io/105661');
sentryClient.patchGlobal();

function getInstagramPageFromLastWeek(offset) {
    var deferred = Q.defer();

    var dateTo = moment().format('YYYY-MM-DD');
    var dateFrom = moment().subtract(7, 'd')
    var lastWeek = dateFrom.format('YYYY-MM-DDTHH:mm:ss');
    console.log(lastWeek);

    elasticSearchClient.search({
        index: 'instagrams',
        type: 'instagram',
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

function getInstagramPostsFromLastWeek(offset, allData) {
    var deferred = Q.defer();

    getInstagramPageFromLastWeek(offset).then(function(data) {
        if (data.length === 0) {
            deferred.resolve(allData);
        } else {
            var newData = allData.concat(data);
            deferred.resolve(getInstagramPostsFromLastWeek(offset + 1, newData));
        }
    });

    return deferred.promise;
}

getInstagramPostsFromLastWeek(0, []).then(function(data) {
    console.log(data.length);
});