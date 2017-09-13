'use strict';

var elasticsearch = require('elasticsearch');
var rp = require('request-promise');
var Q = require('q');
var Twitter = require('twitter');
var raven = require('raven');
var gcloud = require('google-cloud')({
    projectId: 'newsai-1166'
});

// Initialize Google Cloud
var topicName = 'process-twitter-md-feed';
var subscriptionName = 'node-new-md-user-twitter';
var pubsub = gcloud.pubsub();

// Instantiate a sentry client
var sentryClient = new raven.Client('https://9fd2d37a95dc472496b018dd15be1369:195a98802d4f4440852aea96ebb65d2b@sentry.io/103128');
sentryClient.patchGlobal();

// Instantiate a twitter client
var twitterClient = new Twitter({
    consumer_key: 'nu83S4GaW4vrsN6gPoTbSvuMy',
    consumer_secret: 't86zlLxN7mjwHu9OMflX806StaqSFWfLMTOiiFLmOuwI5kUFFE',
    access_token_key: '758002735547609088-bPZJ1mO8nPfHq52FquOh0tsaWa6Fc28',
    access_token_secret: 'NIYOhbJZSFzKNRJGVdtPlzMnzKet9bHdwH08ghw9TmzWr'
});

// Instantiate a elasticsearch client
var elasticSearchClient = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

var newTwitter = require('../../twitter/new-twitter');

function addESActionsToEs(esActions) {
    var deferred = Q.defer();

    elasticSearchClient.bulk({
        body: esActions
    }, function(error, response) {
        if (error) {
            console.log(error);
            sentryClient.captureMessage(error);
            deferred.reject(error);
        }
        deferred.resolve(response);
    });

    return deferred.promise;
}

function getFeedsPageFromEs(indexName, typeName, offset, username) {
    var deferred = Q.defer();

    elasticSearchClient.search({
        index: indexName,
        type: typeName,
        body: {
            "query": {
                "match": {
                    "data.Username": username
                }
            },
            "size": 100,
            "from": 100 * offset,
        }
    }).then(function(resp) {
        var hits = resp.hits.hits;
        deferred.resolve(hits);
    }, function(err) {
        console.error(err.message);
        deferred.reject(err);
    });

    return deferred.promise;
}

function getFeedsFromEs(indexName, typeName, offset, username, allData) {
    var deferred = Q.defer();

    getFeedsPageFromEs(indexName, typeName, offset, username).then(function(data) {
        if (data.length === 0) {
            deferred.resolve(allData);
        } else {
            var newData = allData.concat(data);
            deferred.resolve(getFeedsFromEs(indexName, typeName, offset + 1, username, newData));
        }
    });

    return deferred.promise;
}

function moveSocialDataToMDElastic(tweets, feeds) {
    var deferred = Q.defer();
    var esActions = [];

    for (var i = 0; i < tweets.length; i++) {
        var indexRecord = {
            index: {
                _index: 'tweets',
                _type: 'md-tweet',
                _id: tweets[i]._id
            }
        };

        var dataRecord = tweets[i]._source && tweets[i]._source.data;
        esActions.push(indexRecord);
        esActions.push({
            data: dataRecord
        });
    }

    for (var i = 0; i < tweets.length; i++) {
        var deleteRecord = {
            delete: {
                _index: 'tweets',
                _type: 'tweet',
                _id: tweets[i]._id
            }
        };

        esActions.push(deleteRecord);
    }

    for (var i = 0; i < feeds.length; i++) {
        var indexRecord = {
            index: {
                _index: 'feeds',
                _type: 'md-feed',
                _id: feeds[i]._id
            }
        };

        if (feeds[i]._source.data.Type === 'Tweet') {
            var dataRecord = feeds[i]._source && feeds[i]._source.data;
            esActions.push(indexRecord);
            esActions.push({
                data: dataRecord
            });
        }
    }

    for (var i = 0; i < feeds.length; i++) {
        var deleteRecord = {
            delete: {
                _index: 'feeds',
                _type: 'feed',
                _id: feeds[i]._id
            }
        };

        if (feeds[i]._source.data.Type === 'Tweet') {
            esActions.push(deleteRecord);
        }
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

function transitionTweetsToMd(twitterUsername) {
    var deferred = Q.defer();

    getFeedsFromEs('tweets', 'tweet', 0, twitterUsername, []).then(function(tweets) {
        getFeedsFromEs('feeds', 'feed', 0, twitterUsername, []).then(function(feeds) {
            moveSocialDataToMDElastic(tweets, feeds).then(function(status) {
                deferred.resolve(status);
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
    }, function(error) {
        sentryClient.captureMessage(error);
        console.error(error);
        deferred.reject(error);
    });

    return deferred.promise;
}

// Get a Google Cloud topic
function getTopic(cb) {
    pubsub.createTopic(topicName, function(err, topic) {
        // topic already exists.
        if (err && err.code === 409) {
            return cb(null, pubsub.topic(topicName));
        }
        return cb(err, topic);
    });
}

function processTwitterUsers(data) {
    var allPromises = [];

    var twitterUsernames = data.username.split(',');
    for (var i = 0; i < twitterUsernames.length; i++) {
        var toExecute = newTwitter.processTwitterUser(twitterClient, sentryClient, twitterUsernames[i], 'md-tweet', 'md-feed');
        allPromises.push(toExecute);

        var tweetTransition = transitionTweetsToMd(twitterUsernames[i]);
        allPromises.push(tweetTransition);
    }

    return Q.all(allPromises);
}

// Subscribe to Pub/Sub for this particular topic
function subscribe(cb) {
    var subscription;

    // Event handlers
    function handleMessage(message) {
        cb(null, message);
    }

    function handleError(err) {
        sentryClient.captureMessage(err);
        console.error(err);
    }

    getTopic(function(err, topic) {
        if (err) {
            return cb(err);
        }

        topic.subscribe(subscriptionName, {
            autoAck: true,
            reuseExisting: true
        }, function(err, sub) {
            if (err) {
                return cb(err);
            }

            subscription = sub;

            // Listen to and handle message and error events
            subscription.on('message', handleMessage);
            subscription.on('error', handleError);

            console.log('Listening to ' + topicName +
                ' with subscription ' + subscriptionName);
        });
    });

    // Subscription cancellation function
    return function() {
        if (subscription) {
            // Remove event listeners
            subscription.removeListener('message', handleMessage);
            subscription.removeListener('error', handleError);
            subscription = undefined;
        }
    };
}

// Begin subscription
subscribe(function(err, message) {
    // Any errors received are considered fatal.
    if (err) {
        sentryClient.captureMessage(err);
        console.error(err);
        throw err;
    }
    console.log('Received request to process twitter feed ' + message.data.username);
    processTwitterUsers(message.data)
        .then(function(status) {
            rp('https://hchk.io/1a7203c6-1716-4933-bdc0-673c4cd2d7bd')
                .then(function(htmlString) {
                    console.log('Completed execution for ' + message.data.username);
                })
                .catch(function(err) {
                    console.error(err);
                });
        }, function(error) {
            sentryClient.captureMessage(error);
            console.error(error);
        });
});

// // Code for testing the functions above
// var message = {
//     data: {
//         username: 'abhiagarwal'
//     }
// };

// processTwitterUser(message.data)
//     .then(function(status) {
//         console.log('Completed execution for ' + message.data.username);
//     }, function(error) {
//         console.error(error);
//     });