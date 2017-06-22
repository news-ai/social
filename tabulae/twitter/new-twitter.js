'use strict';

var rp = require('request-promise');
var Q = require('q');
var Twitter = require('twitter');
var raven = require('raven');
var gcloud = require('google-cloud')({
    projectId: 'newsai-1166'
});

// Initialize Google Cloud
var topicName = 'process-twitter-feed';
var subscriptionName = 'node-new-user-twitter';
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

var twitterShared = require('../../twitter/twitter');

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
        var toExecute = twitterShared.processTwitterUser(twitterClient, sentryClient, twitterUsernames[i], 'tweet', 'feed');
        allPromises.push(toExecute);
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