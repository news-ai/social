/*!
 * Syncs single Twitter posts from Twitter API to ES
 */
'use strict';

var Q = require('q');
var elasticsearch = require('elasticsearch');
var moment = require('moment');
var raven = require('raven');

var twitter = require('./twitter');

// Instantiate a elasticsearch client
var elasticSearchClient = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search1.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

// Instantiate a sentry client
var sentryClient = new raven.Client('https://666f957c7dd64957996c1b05675a960a:b942eb7df51d4f8780f55b7d4592a39f@sentry.io/105661');
sentryClient.patchGlobal();

function syncTwitterAndES() {
    var deferred = Q.defer();

    // Get tweets from ES
    twitter.getTweetsFromEsLastWeek(0, []).then(function(data) {
        // Find the tweets from Twitter API
        console.log(data.length);
    }, function (error) {
        sentryClient.captureMessage(error);
        console.error(error);
        deferred.reject(error);
    });

    return deferred.promise;
}

function runUpdates() {
    // Run one initially -- mostly for when testing
    console.log('Beginning run');
    syncTwitterAndES().then(function(status) {
        console.log(status);
    }, function(error) {
        console.error(error);
    })

    // Run feed code every fifteen minutes
    setInterval(function() {
        console.log('Updating Twitter posts');
        syncTwitterAndES().then(function(status) {
            console.log(status);
        }, function(error) {
            sentryClient.captureMessage(error);
            console.error(error);
        })
    }, 60 * 60 * 1000);
}

runUpdates();