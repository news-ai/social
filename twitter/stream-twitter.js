'use strict';

var elasticsearch = require('elasticsearch');
var rp = require('request-promise');
var moment = require('moment');
var Q = require('q');
var Stream = require('user-stream');
var raven = require('raven');

// Instantiate a elasticsearch client
var elasticSearchClient = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search1.newsai.org',
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
        'TwitterLikes': tweet.Likes,
        'TwitterRetweets': tweet.Retweets,
        'Text': tweet.Text,
        'Username': username,
    };
}

function addTweetToEs(tweet, twitterProfile, userInMediaDatabase) {
    var deferred = Q.defer();

    var tweetType = 'tweet';
    var feedType = 'feed';

    if (userInMediaDatabase) {
        tweetType = 'md-tweet';
        feedType = 'md-tweet';
    }

    var esActions = [];

    var coordinates = '';
    if (tweet.coordinates && tweet.coordinates.coordinates && tweet.coordinates.coordinates.length === 2) {
        coordinates = tweet.coordinates.coordinates[0].toString() + ',' + tweet.coordinates.coordinates[1].toString();
    }

    var isRetweeted = false
    if (tweet.retweeted_status && tweet.retweeted_status.created_at) {
        isRetweeted = true;
    }

    var tweetToAdd = {
        'TweetId': tweet.id,
        'TweetIdStr': tweet.id_str,
        'Text': tweet.text,
        'Likes': tweet.favorite_count,
        'Retweets': tweet.retweet_count,
        'Place': tweet.place && tweet.place.full_name || '',
        'Coordinates': coordinates,
        'Retweeted': isRetweeted,
        'CreatedAt': moment(tweet.created_at).format('YYYY-MM-DDTHH:mm:ss')
    };

    var indexRecord = {
        index: {
            _index: 'tweets',
            _type: tweetType,
            _id: tweet.id
        }
    };

    var dataRecord = tweetToAdd;
    dataRecord.Username = twitterProfile.screen_name;
    esActions.push(indexRecord);
    esActions.push({
        data: dataRecord
    });

    indexRecord = {
        index: {
            _index: 'feeds',
            _type: feedType,
            _id: tweet.id
        }
    };
    dataRecord = formatToFeed(tweetToAdd, twitterProfile.screen_name);
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
            deferred.resolve(hits[0]._source.data);
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

function checkIfUserIsInMediaDatabase(twitterProfile) {
    var deferred = Q.defer();
    var username = twitterProfile.screen_name.toLowerCase();

    client.get({
        index: 'md',
        type: 'socialProfiles',
        id: 'twitter-' + username
    }, function (error, response) {
        if (error) {
            deferred.resolve(false);
        } else {
            console.log('Media Database Tweet: ' + username)
            deferred.resolve(true);
        }
    });

    return deferred.promise;
}

function processTweet(tweet) {
    var deferred = Q.defer();

    if (tweet && tweet.user && tweet.user.id) {
        findUsernameFromTwitterId(tweet.user.id).then(function(twitterProfile) {
            checkIfUserIsInMediaDatabase(twitterProfile).then(function(userInMediaDatabase) {
                addTweetToEs(tweet, twitterProfile, userInMediaDatabase).then(function(status) {
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
        processTweet(tweet).then(function(response) {
            rp('https://hchk.io/73a12c55-c81f-4f0e-b0fe-cc0e26c18cd7')
                .then(function(htmlString) {
                    console.log(response);
                })
                .catch(function(err) {
                    console.error(err);
                });
        }, function(error) {
            console.error(error);
            sentryClient.captureMessage(error);
        });
    }
});

// Garbage
// Unsure why it places it as garbage, but process it anyways.
// If it throws an error who cares.
stream.on('garbage', function(tweet) {
    console.log(tweet.id);
    if (!tweet.friends && tweet.id) {
        processTweet(tweet).then(function(response) {
            rp('https://hchk.io/73a12c55-c81f-4f0e-b0fe-cc0e26c18cd7')
                .then(function(htmlString) {
                    console.log(response);
                })
                .catch(function(err) {
                    console.error(err);
                });
        }, function(error) {
            console.error(error);
            sentryClient.captureMessage(error);
        });
    }
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