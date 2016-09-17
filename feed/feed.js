'use strict';

var moment = require('moment');
var Q = require('q');
var gcloud = require('google-cloud')({
    projectId: 'newsai-1166'
});

// Instantiate a datastore client
var datastore = gcloud.datastore();

// Initialize Google Cloud
var pubsub = gcloud.pubsub();
var topicName = 'process-rss-feed';

function getTopic(cb) {
    pubsub.createTopic(topicName, function(err, topic) {
        // topic already exists.
        if (err && err.code === 409) {
            return cb(null, pubsub.topic(topicName));
        }
        return cb(err, topic);
    });
}

function addFeedToPubSub(contactId, url) {
    getTopic(function(err, topic) {
        if (err) {
            console.error('Error occurred while getting pubsub topic', err);
            return;
        }

        topic.publish({
            contactId: contactId,
            url: url
        }, function(err) {
            if (err) {
                console.error('Error occurred while queuing background task', err);
            } else {
                console.info('Feed ' + url + ' sent to ' + topicName + ' pubsub');
            }
        });
    });
}

function getLatestFeeds() {
    var time = moment();
    var fifteenMinutes = moment.duration(15, 'minutes');
    time.subtract(fifteenMinutes);

    var query = datastore.createQuery('Feed');
    var feedQuery = query.filter('Updated', '>', time._d);
}

function runFeeds() {
    setInterval(function() {

    }, 15 * 60 * 1000);
}

runFeeds();