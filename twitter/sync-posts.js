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
var sentryClient = new raven.Client('https://27c00cc5e3ad42c2982d394811eb7633:f2dbda55263b4d40a603442926782cf4@sentry.io/106930');
sentryClient.patchGlobal();

function formatToFeed(tweet) {
    return {
        'CreatedAt': tweet.CreatedAt,
        'Type': 'Tweet',

        // Headlines
        'Title': '',
        'Url': '',
        'Summary': '',
        'FeedURL': '',
        'PublicationId': 0,

        // Tweet specific
        'Username': tweet.Username,
        'TweetId': tweet.TweetId,
        'TweetIdStr': tweet.TweetIdStr,
        'TwitterLikes': tweet.Likes,
        'TwitterRetweets': tweet.Retweets,

        // Instagram + Twitter
        'Text': tweet.Text
    };
}

function addESActionsToEs(esActions) {
    var deferred = Q.defer();

    elasticSearchClient.bulk({
        body: esActions
    }, function(error, response) {
        if (error) {
            sentryClient.captureMessage(error);
            deferred.reject(error);
        }
        deferred.resolve(true);
    });

    return deferred.promise;
}

// Add these tweets to ElasticSearch
// username here is the base parent username.
// Not just a username of any user.
function addToElastic(tweets, tweetIdsToESIdAndUsername) {
    var deferred = Q.defer();

    var tweetsToAdd = [];

    for (var i = tweets.length - 1; i >= 0; i--) {
        var coordinates = '';
        if (tweets[i].coordinates && tweets[i].coordinates.coordinates && tweets[i].coordinates.coordinates.length === 2) {
            coordinates = tweets[i].coordinates.coordinates[0].toString() + ',' + tweets[i].coordinates.coordinates[1].toString();
        }

        var isRetweeted = false
        if (tweets[i].retweeted_status && tweets[i].retweeted_status.created_at) {
            isRetweeted = true;
        }

        tweetsToAdd.push({
            'TweetId': tweets[i].id,
            'TweetIdStr': tweets[i].id_str,
            'Text': tweets[i].text,
            'Likes': tweets[i].favorite_count,
            'Retweets': tweets[i].retweet_count,
            'Place': tweets[i].place && tweets[i].place.full_name || '',
            'Coordinates': coordinates,
            'Retweeted': isRetweeted,
            'CreatedAt': moment(tweets[i].created_at).format('YYYY-MM-DDTHH:mm:ss'), // damn you Twitter and your dates
            'Username': tweetIdsToESIdAndUsername[tweets[i].id_str].Username
        });
    }

    var esActions = [];
    for (var i = tweetsToAdd.length - 1; i >= 0; i--) {
        // Add to tweets endpoint
        var indexRecord = {
            index: {
                _index: 'tweets',
                _type: 'tweet',
                _id: tweetsToAdd[i].TweetId
            }
        };
        var dataRecord = tweetsToAdd[i];
        esActions.push(indexRecord);
        esActions.push({
            data: dataRecord
        });

        // Add to feeds endpoint
        indexRecord = {
            index: {
                _index: 'feeds',
                _type: 'feed',
                _id: tweetsToAdd[i].TweetId
            }
        };
        dataRecord = formatToFeed(tweetsToAdd[i]);
        esActions.push(indexRecord);
        esActions.push({
            data: dataRecord
        });
    }

    var allPromises = [];

    var i, j, temp, chunk = 100;
    for (i = 0, j = esActions.length; i < j; i += chunk) {
        temp = esActions.slice(i, i + chunk);
        var tempFunction = addESActionsToEs(temp);
        allPromises.push(tempFunction);
    }

    return Q.all(allPromises);
}

// Get last 20 tweets for a particular user
function getTweetsFromIds(tweetIds) {
    var deferred = Q.defer();

    twitterClient.get('statuses/lookup', {
        id: tweetIds.join()
    }, function(error, tweets, response) {
        if (!error) {
            deferred.resolve(tweets);
        } else {
            console.error(error);
            sentryClient.captureMessage(error);
            deferred.reject(new Error(error));
        }
    });

    return deferred.promise;
}

function groupTweetsByIds(tweetIds) {
    var allPromises = [];

    var i, j, temp, chunk = 99;
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
        console.log(data.length);
        var tweetIds = [];
        var tweetIdsToESIdAndUsername = {};
        for (var i = 0; i < data.length; i++) {
            tweetIdsToESIdAndUsername[data[i]._source.data.TweetIdStr] = {
                'Id': data[i]._id,
                'Username': data[i]._source.data.Username
            };
            tweetIds.push(data[i]._source.data.TweetIdStr);
        }

        // Get tweets from Twitter
        groupTweetsByIds(tweetIds).then(function (tweets) {
            var allTweets = [].concat.apply([], tweets);

            // Add the data to elasticsearch
            addToElastic(allTweets, tweetIdsToESIdAndUsername).then(function(status) {
                deferred.resolve(status);
            }, function(error) {
                sentryClient.captureMessage(error);
                deferred.reject(error);
            });
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
    }, 15 * 60 * 1000);
}

runUpdates();