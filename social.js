'use strict';

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

function subscribe(cb) {
    var subscription;

    // Event handlers
    function handleMessage(message) {
        cb(null, message);
    }

    function handleError(err) {
        console.error(err);
        sentryClient.captureMessage(err);
    }

    getTopic(function(err, topic) {
        if (err) {
            sentryClient.captureMessage(err);
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