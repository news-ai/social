'use strict';

var elasticsearch = require('elasticsearch');
var moment = require('moment');
var Q = require('q');

// Instantiate a elasticsearch client
var elasticSearchClient = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search1.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

var twitterTimeseries = require('../time-series/twitter');

var twitterShared = exports;

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

        // Tweet specific
        'Username': username,
        'TweetId': tweet.TweetId,
        'TweetIdStr': tweet.TweetIdStr,
        'TwitterLikes': tweet.Likes,
        'TwitterRetweets': tweet.Retweets,

        // Instagram + Twitter
        'Text': tweet.Text
    };
}

// Get last 20 tweets for a particular user
function getTweetsFromUsername(twitterClient, sentryClient, username) {
    var deferred = Q.defer();

    twitterClient.get('statuses/user_timeline', {
        screen_name: username,
        count: 25
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

// Follow this user on Twitter to stream tweets
function followOnTwitter(twitterClient, sentryClient, user) {
    var deferred = Q.defer();

    twitterClient.post('friendships/create', {
        user_id: user.id,
        follow: true
    }, function(error, response) {
        if (error) {
            sentryClient.captureMessage(error);
            deferred.reject(error);
        }

        // Add user to timeseries
        var twitterProfile = [user]
        twitterTimeseries.addTwitterUsersToTimeSeries(twitterProfile).then(function(status) {
            deferred.resolve(status);
        }, function(error) {
            sentryClient.captureMessage(error);
            deferred.reject(error);
        });
    });

    return deferred.promise;
}

// Add these tweets to ElasticSearch
// username here is the base parent username.
// Not just a username of any user.
function addToElastic(sentryClient, username, tweets, tweetESType, feedESType) {
    var deferred = Q.defer();

    var user = tweets[0].user;
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
            'CreatedAt': moment(tweets[i].created_at).format('YYYY-MM-DDTHH:mm:ss') // damn you Twitter and your dates
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
        dataRecord.Username = username;
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
        dataRecord = formatToFeed(tweetsToAdd[i], username);
        esActions.push(indexRecord);
        esActions.push({
            data: dataRecord
        });
    }

    // Add user to ElasticSearch as well
    var indexRecord = {
        index: {
            _index: 'tweets',
            _type: 'user',
            _id: username
        }
    };
    var dataRecord = user;
    dataRecord.Username = username;
    esActions.push(indexRecord);
    esActions.push({
        data: dataRecord
    });

    elasticSearchClient.bulk({
        body: esActions
    }, function(error, response) {
        if (error || response && response.errors) {
            if (!error) {
                error = 'Could not send data to ES';
            }
            sentryClient.captureMessage(error);
            deferred.reject(error);
        } else {
            twitterTimeseries.addTwitterPostsToTimeSeries(tweets).then(function(status) {
                deferred.resolve(user);
            }, function (error) {
                sentryClient.captureMessage(error);
                deferred.reject(error);
            });
        }
    });

    return deferred.promise;
}

// Process a particular Twitter user
function processTwitterUser(twitterClient, sentryClient, username, tweetESType, feedESType) {
    var deferred = Q.defer();

    // Get tweets for a user
    getTweetsFromUsername(twitterClient, sentryClient, username).then(function(tweets) {
        // Add tweets to elasticsearch
        addToElastic(sentryClient, username, tweets, tweetESType, feedESType).then(function(user) {
            if (user) {
                // Follow the user on the NewsAIHQ Twitter so we can stream the
                // Tweets later.
                followOnTwitter(twitterClient, sentryClient, user).then(function(response) {
                    var apiData = {
                        'network': 'Twitter',
                        'username': username,
                        'fullname': user.name || ''
                    };
                    deferred.resolve(true);
                }, function(error) {
                    sentryClient.captureMessage(error);
                    deferred.reject(error);
                });
            } else {
                var error = new Error('Elasticsearch add failed');
                sentryClient.captureMessage(error);
                deferred.reject(error);
            }
        }, function(error) {
            sentryClient.captureMessage(error);
            deferred.reject(error);
        });

    }, function(error) {
        sentryClient.captureMessage(error);
        deferred.reject(error);
    });

    return deferred.promise;
}

twitterShared.processTwitterUser = processTwitterUser;