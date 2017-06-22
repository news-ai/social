'use strict';

var moment = require('moment');
var rp = require('request-promise');
var Q = require('q');
var elasticsearch = require('elasticsearch');
var raven = require('raven');
var gcloud = require('google-cloud')({
    projectId: 'newsai-1166'
});

// Instantiate a elasticsearch client
var client = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search1.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

// Instantiate a datastore client
var datastore = gcloud.datastore();

// Initialize Google Cloud
var pubsub = gcloud.pubsub();
var topicName = 'process-rss-feed';

// Instantiate a sentry client
var sentryClient = new raven.Client('https://c69c2a293ace4f2194b24df6f8d9f865:c61da655d1274dc0aa4dd52bb7c36f3a@sentry.io/103130');
sentryClient.patchGlobal();

function getTopic(cb) {
    pubsub.createTopic(topicName, function(err, topic) {
        // topic already exists.
        if (err && err.code === 409) {
            return cb(null, pubsub.topic(topicName));
        }
        return cb(err, topic);
    });
}

function addFeedToPubSub(publicationId, url) {
    var deferred = Q.defer();

    getTopic(function(err, topic) {
        if (err) {
            deferred.reject(new Error(err));
            console.error('Error occurred while getting pubsub topic', err);
            sentryClient.captureMessage(err);
        } else {
            topic.publish({
                data: {
                    publicationId: publicationId,
                    url: url,
                }
            }, function(err) {
                if (err) {
                    deferred.reject(new Error(err));
                    console.error('Error occurred while queuing background task', err);
                    sentryClient.captureMessage(err);
                } else {
                    deferred.resolve(true);
                    console.info('Feed ' + url + ' sent to ' + topicName + ' pubsub');
                }
            });
        }
    });

    return deferred.promise;
}

function addFeedToElaticsearch(item) {
    var deferred = Q.defer();

    var esActions = [];
    var indexRecord = {
        index: {
            _index: 'rssfeeds',
            _type: 'feed',
            _id: item.key.id
        }
    };
    var dataRecord = item.data;

    esActions.push(indexRecord);
    esActions.push({
        data: dataRecord
    });

    client.bulk({
        body: esActions
    }, function(error, response) {
        if (error) {
            console.error(error);
            sentryClient.captureMessage(error);
            deferred.resolve(false);
        }
        deferred.resolve(true);
    });

    return deferred.promise;
}

function getLatestFeeds() {
    var time = moment();
    var fifteenMinutes = moment.duration(15, 'minutes');
    time.subtract(fifteenMinutes);

    var query = datastore.createQuery('Feed');
    var feedQuery = query.filter('Updated', '<', time._d);
    var feedMap = {};

    feedQuery.run(function(err, entities) {
        entities.forEach(function(item) {
            if (!(item.data.FeedURL in feedMap)) {
                feedMap[item.data.FeedURL] = true;
                addFeedToPubSub(item.data.PublicationId, item.data.FeedURL).then(function(status) {
                    addFeedToElaticsearch(item).then(function (esStatus) {
                        // Change the `Updated` time to now
                        item.data.Updated = moment()._d;
                        datastore.save({
                            key: item.key,
                            data: item.data
                        }, function(err) {
                            if (err) {
                                console.error(err);
                                sentryClient.captureMessage(err);
                            }
                        });
                    }, function(error) {
                        console.error(error);
                        sentryClient.captureMessage(error);
                    });
                }, function(error) {
                    console.error(error);
                    sentryClient.captureMessage(error);
                });
            }
        });
    });
}

function runFeeds() {
    // Run one initially -- mostly for when testing
    console.log('Beginning run');
    getLatestFeeds();

    // Run feed code every fifteen minutes
    setInterval(function() {
        console.log('Processing feeds');
        getLatestFeeds();

        rp('https://hchk.io/57057b83-2bf9-454d-ad35-547b6db86d81')
            .then(function(htmlString) {
                console.log('Completed execution');
            })
            .catch(function(err) {
                console.error(err);
            });
    }, 15 * 60 * 1000);
}

runFeeds();