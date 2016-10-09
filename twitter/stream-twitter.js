'use strict';

var elasticsearch = require('elasticsearch');
var rp = require('request-promise');
var moment = require('moment');
var Q = require('q');
var Stream = require('user-stream');
var raven = require('raven');

// Instantiate a elasticsearch client
var elasticSearchClient = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

// Instantiate a sentry client
var sentryClient = new raven.Client('https://9af56ecaeca547abb4aa9f1bed0626d9:8146296e132a4dd2808b0babdaebfc4c@sentry.io/103129');
sentryClient.patchGlobal();

// Initialize Twitter client and Twitter stream
var stream = new Stream({
    consumer_key: 'nu83S4GaW4vrsN6gPoTbSvuMy',
    consumer_secret: 't86zlLxN7mjwHu9OMflX806StaqSFWfLMTOiiFLmOuwI5kUFFE',
    access_token_key: '758002735547609088-bPZJ1mO8nPfHq52FquOh0tsaWa6Fc28',
    access_token_secret: 'NIYOhbJZSFzKNRJGVdtPlzMnzKet9bHdwH08ghw9TmzWr'
});
stream.stream();

function formatToFeed(tweet, username) {
    return {
        'CreatedAt': tweet.CreatedAt,
        'Type': 'Tweet',

        // Headlines
        'Title': '',
        'Url': '',
        'Summary': '',
        'FeedURL': '',
        'PublicationId': 0,

        // Tweet
        'TweetId': tweet.TweetId,
        'TweetIdStr': tweet.TweetIdStr,
        'Text': tweet.Text,
        'Username': username,
    };
}

function addTweetToEs(tweet, username) {
    var deferred = Q.defer();

    var esActions = [];

    var tweetToAdd = {
        'TweetId': tweet.id,
        'TweetIdStr': tweet.id_str,
        'Text': tweet.text,
        'CreatedAt': moment(tweet.created_at).format('YYYY-MM-DDTHH:mm:ss')
    };

    var indexRecord = {
        index: {
            _index: 'tweets',
            _type: 'tweet',
            _id: tweet.id
        }
    };

    var dataRecord = tweetToAdd;
    dataRecord.Username = username;
    esActions.push(indexRecord);
    esActions.push({
        data: dataRecord
    });

    indexRecord = {
        index: {
            _index: 'feeds',
            _type: 'feed',
            _id: tweet.id
        }
    };
    dataRecord = formatToFeed(tweetToAdd, username);
    esActions.push(indexRecord);
    esActions.push({
        data: dataRecord
    });

    elasticSearchClient.bulk({
        body: esActions
    }, function(error, response) {
        if (error) {
            console.error(error);
            sentryClient.captureMessage(error);
            deferred.reject(error);
        }
        deferred.resolve(true);
    });

    return deferred.promise;
}

function findUsernameFromTwitterId(twitterId) {
    var deferred = Q.defer();

    elasticSearchClient.search({
        q: 'data.id:' + twitterId
    }).then(function(body) {
        var hits = body.hits.hits;
        if (hits.length > 0) {
            deferred.resolve(hits[0]._source.data.screen_name);
        } else {
            var error = 'Did not get any hits';
            console.error(error);
            sentryClient.captureMessage(error);
            deferred.reject(error);
        }
    }, function(error) {
        console.error(error);
        sentryClient.captureMessage(error);
        deferred.reject(error);
    });

    return deferred.promise;
}

function processTweet(tweet) {
    var deferred = Q.defer();

    if (tweet && tweet.user && tweet.user.id) {
        findUsernameFromTwitterId(tweet.user.id).then(function(username) {
            addTweetToEs(tweet, username).then(function(status) {
                if (status) {
                    deferred.resolve(true);
                } else {
                    var error = 'Elasticsearch add failed';
                    console.error(error);
                    sentryClient.captureMessage(error);
                    deferred.reject(error);
                }
            }, function(error) {
                console.error(error);
                sentryClient.captureMessage(error);
                deferred.reject(error);
            });
        }, function(error) {
            console.error(error);
            sentryClient.captureMessage(error);
            deferred.reject(error);
        });
    }

    return deferred.promise;
}

/**
 * Events:
 * - data
 * - garbage
 * - close
 * - error
 * - connected
 * - heartbeat
 */

// Error checking
stream.on('connected', function(error) {
    console.log('Connected to stream');
});

// Incoming tweet for a particular user - add to ElasticSearch
stream.on('data', function(tweet) {
    console.log(tweet.id);
    if (!tweet.friends) {
        processTweet(tweet).then(function (response) {
            rp('https://hchk.io/73a12c55-c81f-4f0e-b0fe-cc0e26c18cd7')
                .then(function (htmlString) {
                    console.log(response);
                })
                .catch(function (err) {
                    console.error(err);
                });
        }, function (error) {
            console.error(error);
            sentryClient.captureMessage(error);
        });
    }
});

// Garbage
stream.on('garbage', function(data) {
    // Restart stream
    console.log('[GARBAGE]: ' + data);
});

// Error checking
stream.on('error', function(error) {
    // Restart stream
    console.log('[ERROR]: ' + error);
    sentryClient.captureMessage('[ERROR]: ' + error);
});

// Error checking - restart stream
stream.on('close', function(error) {
    // Restart stream
    console.log('[STREAM CLOSED]: ' + error);
    sentryClient.captureMessage('[STREAM CLOSED]: ' + error);
    stream.stream();
});