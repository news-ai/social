/*!
 * Syncs single Twitter posts from Twitter API to ES
 */
'use strict';

var Q = require('q');
var elasticsearch = require('elasticsearch');
var moment = require('moment');
var Twitter = require('twitter');
var raven = require('raven');

var twitter = require('./twitter');

// Instantiate a elasticsearch client
var elasticSearchClient = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search1.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

// Instantiate a twitter client
var twitterClient = new Twitter({
    consumer_key: 'nu83S4GaW4vrsN6gPoTbSvuMy',
    consumer_secret: 't86zlLxN7mjwHu9OMflX806StaqSFWfLMTOiiFLmOuwI5kUFFE',
    access_token_key: '758002735547609088-bPZJ1mO8nPfHq52FquOh0tsaWa6Fc28',
    access_token_secret: 'NIYOhbJZSFzKNRJGVdtPlzMnzKet9bHdwH08ghw9TmzWr'
});

// Instantiate a sentry client
var sentryClient = new raven.Client('https://666f957c7dd64957996c1b05675a960a:b942eb7df51d4f8780f55b7d4592a39f@sentry.io/105661');
sentryClient.patchGlobal();

// Get last 20 tweets for a particular user
function getTweetFromId(tweetId) {
    var deferred = Q.defer();

    twitterClient.get('statuses/show', {
        id: tweetId
    }, function(error, tweet, response) {
        if (!error) {
            console.log(tweet);
            deferred.resolve(tweet);
        } else {
            console.error(error);
            sentryClient.captureMessage(error);
            deferred.reject(new Error(error));
        }
    });

    return deferred.promise;
}

function getTweetsFromIds(tweetIds) {
    var allPromises = [];
    for (var i = tweetIds.length - 1; i >= 0; i--) {
        var toExecute = getTweetFromId(tweetIds[i].TwitterId);
        allPromises.push(toExecute);
    }
    return Q.all(allPromises);
}

function groupTweetsByIds(tweetIds) {
    var allPromises = [];
    var i, j, temp, chunk = 100;
    for (i = 0, j = tweetIds.length; i < j; i += chunk) {
        temp = tweetIds.slice(i, i + chunk);
        var toExecute = getTweetsFromIds(temp);
        allPromises.push(toExecute);
    } 
    return Q.all(allPromises);
}

function syncTwitterAndES() {
    var deferred = Q.defer();

    // Get tweets from ES
    twitter.getTweetsFromEsLastWeek(0, []).then(function(data) {
        // Find the tweets from Twitter API
        console.log(data.length);
        var tweetIds = []
        for (var i = 0; i < data.length; i++) {
            var tweet = {
                'ESId': data[i]._id,
                'TwitterId': data[i]._source.data.TweetIdStr
            };
            tweetIds.push(tweet);
        }

        var x = [tweetIds[0], tweetIds[1]];
        console.log(x);

        groupTweetsByIds(x).then(function (tweets) {
            console.log(tweets.length);
            console.log(tweets[0]);
        }, function (error) {
            sentryClient.captureMessage(error);
            console.error(error);
            deferred.reject(error);
        });
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