'use strict';

var elasticsearch = require('elasticsearch');
var Q = require('q');
var Twitter = require('twitter');
// var gcloud = require('google-cloud')({
//     projectId: 'newsai-1166'
// });


// // Instantiate a elasticsearch client
// var client = new elasticsearch.Client({
//     host: 'https://newsai:XkJRNRx2EGCd6@search.newsai.org',
//     // log: 'trace',
//     rejectUnauthorized: false
// });

// Initialize Google Cloud
// var topicName = 'process-twitter-feed';
// var subscriptionName = 'node-new-user-twitter';
// var pubsub = gcloud.pubsub();

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
            console.log(tweets);
        } else {
            console.error(tweets);
        }
    });

    return deferred.promise;
}

// Add these tweets to ElasticSearch
function addToElastic(contactId, tweets) {
    var deferred = Q.defer();

    return deferred.promise;
}

// Follow this user on Twitter to stream tweets
function followOnTwitter(twitterUser) {
    var deferred = Q.defer();

    return deferred.promise;
}

// Process a particular Twitter user
function processTwitterUser(data) {
    var deferred = Q.defer();

    // Get tweets for a user
    getTweetsFromUsername(data.username).then(function(tweets) {
        // Add tweets to elasticsearch
        addToElastic(data.contactId, tweets).then(function(status) {
            if (status) {
                // Follow the user on the NewsAIHQ Twitter so we can stream the
                // Tweets later.
                followOnTwitter(data.username).then(function(response) {
                    deferred.resolve(true);
                }, function(error) {
                    console.error(error);
                    deferred.resolve(false);
                    throw new Error(error);
                });
            } else {
                var error = 'Elasticsearch add failed';
                console.error(error);
                deferred.resolve(false);
                throw new Error(error);
            }
        }, function(error) {
            console.error(error);
            deferred.resolve(false);
            throw new Error(error);
        });

    }, function(error) {
        console.error(error);
        deferred.resolve(false);
        throw new Error(error);
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

// // Begin subscription
// subscribe(function(err, message) {
//     // Any errors received are considered fatal.
//     if (err) {
//         console.error(err);
//         throw err;
//     }
//     console.log('Received request to process twitter feed ' + message.data.username);
//     processTwitterUser(message.data)
//         .then(function(status) {
//             console.log('Completed execution for ' + message.data.username);
//         }, function(error) {
//             console.error(error);
//         });
// });

var message = {
    data: {
        username: 'kanarula',
        contactId: 4896083670990848
    }
};

processTwitterUser(message.data)
    .then(function(status) {
        console.log('Completed execution for ' + message.data.username);
    }, function(error) {
        console.error(error);
    });