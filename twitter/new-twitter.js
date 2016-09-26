'use strict';

var elasticsearch = require('elasticsearch');
var moment = require('moment');
var Q = require('q');
var Twitter = require('twitter');
var gcloud = require('google-cloud')({
    projectId: 'newsai-1166'
});

// Instantiate a elasticsearch client
var elasticSearchClient = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

// Initialize Google Cloud
var topicName = 'process-twitter-feed';
var subscriptionName = 'node-new-user-twitter';
var pubsub = gcloud.pubsub();

var twitterClient = new Twitter({
    consumer_key: 'nu83S4GaW4vrsN6gPoTbSvuMy',
    consumer_secret: 't86zlLxN7mjwHu9OMflX806StaqSFWfLMTOiiFLmOuwI5kUFFE',
    access_token_key: '758002735547609088-bPZJ1mO8nPfHq52FquOh0tsaWa6Fc28',
    access_token_secret: 'NIYOhbJZSFzKNRJGVdtPlzMnzKet9bHdwH08ghw9TmzWr'
});

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

// Get last 20 tweets for a particular user
function getTweetsFromUsername(username) {
    var deferred = Q.defer();

    twitterClient.get('statuses/user_timeline', {
        screen_name: username,
        count: 10
    }, function(error, tweets, response) {
        if (!error) {
            deferred.resolve(tweets);
        } else {
            deferred.reject(new Error(error));
        }
    });

    return deferred.promise;
}

// Add these tweets to ElasticSearch
// username here is the base parent username.
// Not just a username of any user.
function addToElastic(username, tweets) {
    var deferred = Q.defer();

    var user = tweets[0].user;
    var tweetsToAdd = [];

    for (var i = tweets.length - 1; i >= 0; i--) {
        tweetsToAdd.push({
            'TweetId': tweets[i].id,
            'Text': tweets[i].text,
            'CreatedAt': moment(tweets[i].created_at).format('YYYY-MM-DDTHH:mm:ss') // damn you Twitter and your dates
        });
    }

    var esActions = [];
    for (var i = tweetsToAdd.length - 1; i >= 0; i--) {
        var indexRecord = {
            index: {
                _index: 'tweets',
                _type: 'tweet',
                _id: tweetsToAdd[i].Id
            }
        };
        var dataRecord = tweetsToAdd[i];
        dataRecord.Username = username;
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
        if (error) {
            deferred.reject(error);
        }
        deferred.resolve(user);
    });

    return deferred.promise;
}

// Follow this user on Twitter to stream tweets
function followOnTwitter(user) {
    var deferred = Q.defer();

    twitterClient.post('friendships/create', {
        user_id: user.id,
        follow: true
    }, function(error, response) {
        if (!error) {
            deferred.resolve(true);
        }
        deferred.reject(error);
    });

    return deferred.promise;
}

// Process a particular Twitter user
function processTwitterUser(data) {
    var deferred = Q.defer();

    // Get tweets for a user
    getTweetsFromUsername(data.username).then(function(tweets) {
        // Add tweets to elasticsearch
        addToElastic(data.username, tweets).then(function(user) {
            if (user) {
                // Follow the user on the NewsAIHQ Twitter so we can stream the
                // Tweets later.
                followOnTwitter(user).then(function(response) {
                    deferred.resolve(true);
                }, function(error) {
                    deferred.reject(error);
                });
            } else {
                deferred.reject(new Error('Elasticsearch add failed'));
            }
        }, function(error) {
            deferred.reject(error);
        });

    }, function(error) {
        deferred.reject(error);
    });

    return deferred.promise;
}

// Subscribe to Pub/Sub for this particular topic
function subscribe(cb) {
    var subscription;

    // Event handlers
    function handleMessage(message) {
        cb(null, message);
    }

    function handleError(err) {
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
        console.error(err);
        throw err;
    }
    console.log('Received request to process twitter feed ' + message.data.username);
    processTwitterUser(message.data)
        .then(function(status) {
            console.log('Completed execution for ' + message.data.username);
        }, function(error) {
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