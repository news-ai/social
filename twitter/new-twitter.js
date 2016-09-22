'use strict';

var elasticsearch = require('elasticsearch');
var Q = require('q');
var gcloud = require('google-cloud')({
    projectId: 'newsai-1166'
});


// Instantiate a elasticsearch client
var client = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

// Initialize Google Cloud
var topicName = 'process-twitter-feed';
var subscriptionName = 'node-new-user-twitter';
var pubsub = gcloud.pubsub();

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

    return deferred.promise;
}

// Add these tweets to ElasticSearch
function addToElastic(publicationId, content) {
    var deferred = Q.defer();

    return deferred.promise;
}

// Follow this user on Twitter to stream tweets
function followOnTwitter(twitterUser, content) {
    var deferred = Q.defer();

    return deferred.promise;
}

// Process a particular Twitter user
function processTwitterUser(data) {
    var deferred = Q.defer();

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
    console.log('Received request to process twitter feed ' + message.data.url);
    processTwitterUser(message.data)
        .then(function(status) {
            console.log('Completed execution for ' + message.data.url);
        }, function(error) {
            console.error(error);
        });
});
