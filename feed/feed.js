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
    var deferred = Q.defer();

    getTopic(function(err, topic) {
        if (err) {
            deferred.reject(new Error(err));
            console.error('Error occurred while getting pubsub topic', err);
        } else {
            topic.publish({
                contactId: contactId,
                url: url
            }, function(err) {
                if (err) {
                    deferred.reject(new Error(err));
                    console.error('Error occurred while queuing background task', err);
                } else {
                    deferred.resolve(true);
                    console.info('Feed ' + url + ' sent to ' + topicName + ' pubsub');
                }
            });
        }
    });

    return deferred.promise;
}

function getLatestFeeds() {
    var time = moment();
    var fifteenMinutes = moment.duration(15, 'minutes');
    time.subtract(fifteenMinutes);

    var query = datastore.createQuery('Feed');
    var feedQuery = query.filter('Updated', '>', time._d);

    feedQuery.run(function(err, entities) {
        console.log(entities);
        entities.forEach(function(item) {
            addFeedToPubSub(item.data.ContactId, item.data.FeedURL)
                .then(function(status) {
                    // Change the `Updated` time to now
                    item.data.Updated = ???;
                    datastore.save({
                        key: item.key,
                        data: item.data
                    }, function(err) {
                        if (err) {
                            console.error(err);
                        }
                    });
                }, function(error) {
                    console.error(error);
                });
        });
    });
}

function runFeeds() {
    setInterval(function() {

    }, 15 * 60 * 1000);
}

runFeeds();