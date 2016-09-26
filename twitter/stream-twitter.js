'use strict';

var elasticsearch = require('elasticsearch');
var moment = require('moment');
var Q = require('q');
var Stream = require('user-stream');

// Instantiate a elasticsearch client
var elasticSearchClient = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

// Initialize Twitter client and Twitter stream
var stream = new Stream({
    consumer_key: 'nu83S4GaW4vrsN6gPoTbSvuMy',
    consumer_secret: 't86zlLxN7mjwHu9OMflX806StaqSFWfLMTOiiFLmOuwI5kUFFE',
    access_token_key: '758002735547609088-bPZJ1mO8nPfHq52FquOh0tsaWa6Fc28',
    access_token_secret: 'NIYOhbJZSFzKNRJGVdtPlzMnzKet9bHdwH08ghw9TmzWr'
});
stream.stream();

function addTweetToEs(tweet, username) {
    var deferred = Q.defer();

    var esActions = [];

    var tweetToAdd = {
        'TweetId': tweet.id,
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

    elasticSearchClient.bulk({
        body: esActions
    }, function(error, response) {
        if (error) {
            console.error(error);
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
            deferred.reject(error);
        }
    }, function(error) {
        console.error(error);
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
                    deferred.reject(error);
                }
            }, function(error) {
                console.error(error);
                deferred.reject(error);
            });
        }, function(error) {
            console.error(error);
            deferred.reject(error);
        });
    } else {
        var error = 'Not supporting removing tweets yet';
        console.error(error);
        deferred.reject(error);
    }

    return deferred.promise;
}

// Incoming tweet for a particular user - add to ElasticSearch
stream.on('data', function(tweet) {
    console.log(tweet.id);
    if (!tweet.friends) {
        processTweet(tweet).then(function (response) {
            console.log(response);
        }, function (error) {
            console.error(error);
        });
    }
});