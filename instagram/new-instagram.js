'use strict';

var Q = require('q');
var request = require('request');
var elasticsearch = require('elasticsearch');
var moment = require('moment');
var Twitter = require('twitter');
var raven = require('raven');
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
var topicName = 'process-instagram-feed';
var subscriptionName = 'node-new-user-instagram';
var pubsub = gcloud.pubsub();

// Instantiate a sentry client
var sentryClient = new raven.Client('https://4db5dd699d4a4267ab6f56fa97a9ee5c:9240e5b57b864de58f514b6d40e7e5a7@sentry.io/103131');
sentryClient.patchGlobal();

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

function formatToFeed(post, username) {
    return {
        'CreatedAt': post.CreatedAt,
        'Type': 'Instagram',

        // Headlines
        'Title': '',
        'Url': '',
        'Summary': '',
        'FeedURL': '',
        'PublicationId': 0,

        // Tweet
        'TweetId': 0,
        'Username': '',

        // Tweet + Instagram
        'Text': post.Caption,

        // Instagram
        'InstagramUsername': username,
        'InstagramId': post.InstagramId,
        'InstagramImage': post.Image,
        'InstagramVideo': post.Video,
        'InstagramLink': post.Link,
        'InstagramLikes': post.Likes,
        'InstagramComments': post.Comments
    };
}

// Add these instagram posts to ElasticSearch
// username here is the base parent username.
// Not just a username of any user.
function addToElastic(username, posts) {
    var deferred = Q.defer();

    var user = posts.data[0].user;
    var esActions = [];

    // Look through all the instagram data
    for (var i = posts.data.length - 1; i >= 0; i--) {
        var newInstagramPost = {};

        newInstagramPost.CreatedAt = moment.unix(parseInt(posts.data[i].created_time,10)).format('YYYY-MM-DDTHH:mm:ss');
        newInstagramPost.Video = posts.data[i].videos && posts.data[i].videos.standard_resolution && posts.data[i].videos.standard_resolution.url || '';
        newInstagramPost.Image = posts.data[i].images && posts.data[i].images.standard_resolution && posts.data[i].images.standard_resolution.url || '';
        newInstagramPost.Location = posts.data[i].location && posts.data[i].location.name || '';

        var coordinates = '';
        if (posts.data[i].location && posts.data[i].location.latitude && posts.data[i].location.longitude) {
            coordinates = posts.data[i].location.latitude.toString() + ',' + posts.data[i].location.longitude.toString();
        }

        newInstagramPost.Coordinates = coordinates;
        newInstagramPost.InstagramId = posts.data[i].id || '';
        newInstagramPost.Caption = posts.data[i].caption && posts.data[i].caption.text || '';
        newInstagramPost.Likes = posts.data[i].likes && posts.data[i].likes.count || 0;
        newInstagramPost.Comments = posts.data[i].comments && posts.data[i].comments.count || 0;
        newInstagramPost.Link = posts.data[i].link || '';

        var tags = [];
        if (posts.data[i].tags && posts.data[i].tags.length > 0) {
            tags = posts.data[i].tags;
        }

        newInstagramPost.Tags = tags;

        // Add to instagram endpoint
        var indexRecord = {
            index: {
                _index: 'instagrams',
                _type: 'instagram',
                _id: posts.data[i].id
            }
        };
        var dataRecord = newInstagramPost;
        dataRecord.Username = username;
        esActions.push(indexRecord);
        esActions.push({
            data: dataRecord
        });

        // Add to feeds endpoint
        indexRecord = {
            index: {
                _index: 'feeds',
                _type: 'feed',
                _id: posts.data[i].id
            }
        };
        dataRecord = formatToFeed(newInstagramPost, username);
        esActions.push(indexRecord);
        esActions.push({
            data: dataRecord
        });
    }

    // Add user to ElasticSearch as well
    var indexRecord = {
        index: {
            _index: 'instagrams',
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
            sentryClient.captureMessage(error);
            deferred.reject(error);
        }
        deferred.resolve(user);
    });

    return deferred.promise;
}

function getInstagramFromUsername(access_token, username) {
    var deferred = Q.defer();

    request('https://api.instagram.com/v1/users/self/media/recent/?access_token=' + access_token, function(error, response, body) {
        if (!error && response.statusCode == 200) {
            var instagramMedia = JSON.parse(body);
            deferred.resolve(instagramMedia);
        } else {
            console.error(error);
            sentryClient.captureMessage(error);
            deferred.reject(new Error(error));
        }
    })

    return deferred.promise;
}

// Process a particular Instagram user
function processInstagramUser(data) {
    var deferred = Q.defer();

    // Get tweets for a user
    getInstagramFromUsername(data.access_token, data.username).then(function(posts) {
        // Add instagram posts to elasticsearch
        addToElastic(data.username, posts).then(function(status) {
            if (status) {
                deferred.resolve(status);
            } else {
                var error = 'Could not add instagram posts to ES'
                sentryClient.captureMessage(error);
                deferred.reject(error);
            }
        }, function(error) {
            sentryClient.captureMessage(error);
            deferred.reject(error);
        });
    }, function(error) {
        sentryClient.captureMessage(error);
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
        sentryClient.captureMessage(err);
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
        sentryClient.captureMessage(err);
        throw err;
    }
    console.log('Received request to process twitter feed ' + message.data.username);
    processInstagramUser(message.data)
        .then(function(status) {
            console.log('Completed execution for ' + message.data.username);
        }, function(error) {
            console.error(error);
            sentryClient.captureMessage(error);
        });
});

// // Code for testing the functions above
// var message = {
//     data: {
//         access_token: '43004312.4314d27.3e8c7280a4ec49119e240d8cbaaa89c4',
//         username: 'abhiagarwal'
//     }
// };

// processInstagramUser(message.data)
//     .then(function(status) {
//         console.log('Completed execution for ' + message.data.username);
//     }, function(error) {
//         console.error(error);
//     });