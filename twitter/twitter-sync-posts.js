/*!
 * Syncs single Twitter posts from Twitter API to ES
 */
'use strict';

var Q = require('q');
var elasticsearch = require('elasticsearch');
var rp = require('request-promise');
var moment = require('moment');
var Twitter = require('twitter');
var raven = require('raven');

var twitter = require('./twitter');
var twitterTimeseries = require('../time-series/twitter');

var twitterShared = exports;

// Instantiate a elasticsearch client
var elasticSearchClient = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

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

function addESActionsToEs(sentryClient, esActions) {
    var deferred = Q.defer();

    elasticSearchClient.bulk({
        body: esActions
    }, function(error, response) {
        if (error) {
            console.error(error);
            sentryClient.captureMessage(error);
            deferred.reject(error);
        }
        deferred.resolve(response);
    });

    return deferred.promise;
}

// Add these tweets to ElasticSearch
// username here is the base parent username.
// Not just a username of any user.
function addToElastic(sentryClient, tweets, tweetIdsToESIdAndUsername, tweetESType, feedESType) {
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
                _type: tweetESType,
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
                _type: feedESType,
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
        var tempFunction = addESActionsToEs(sentryClient, temp);
        allPromises.push(tempFunction);
    }

    return Q.all(allPromises);
}

// Get last 20 tweets for a particular user
function getTweetsFromIds(twitterClient, sentryClient, tweetIds) {
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

function groupTweetsByIds(twitterClient, sentryClient, tweetIds) {
    var allPromises = [];

    var i, j, temp, chunk = 99;
    for (i = 0, j = tweetIds.length; i < j; i += chunk) {
        temp = tweetIds.slice(i, i + chunk);
        var toExecute = getTweetsFromIds(twitterClient, sentryClient, temp);
        allPromises.push(toExecute);
    }

    return Q.all(allPromises);
}

function syncTwitterAndES(twitterClient, sentryClient, tweetESType, feedESType) {
    var deferred = Q.defer();

    // Get tweets from ES
    twitter.getTweetsFromEsLastDay(0, [], tweetESType).then(function(data) {
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
        groupTweetsByIds(twitterClient, sentryClient, tweetIds).then(function(tweets) {
            var allTweets = [].concat.apply([], tweets);

            // Add the data to elasticsearch
            addToElastic(sentryClient, allTweets, tweetIdsToESIdAndUsername, tweetESType, feedESType).then(function(status) {
                twitterTimeseries.addTwitterPostsToTimeSeries(allTweets).then(function(timeseriesData) {
                    // Health check
                    deferred.resolve(timeseriesData);
                }, function(error) {
                    sentryClient.captureMessage(error);
                    console.error(error);
                    deferred.reject(error);
                });
            }, function(error) {
                sentryClient.captureMessage(error);
                deferred.reject(error);
            });
        }, function(error) {
            sentryClient.captureMessage(error);
            console.error(error);
            deferred.reject(error);
        });
    }, function(error) {
        sentryClient.captureMessage(error);
        console.error(error);
        deferred.reject(error);
    });

    return deferred.promise;
}

twitterShared.syncTwitterAndES = syncTwitterAndES;