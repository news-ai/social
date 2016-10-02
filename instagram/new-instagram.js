'use strict';

var Q = require('q');
var request = require('request');
var elasticsearch = require('elasticsearch');
var moment = require('moment');
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
var topicName = 'process-instagram-feed';
var subscriptionName = 'node-new-user-instagram';
var pubsub = gcloud.pubsub();

function formatToFeed(post, username) {
    return {
        'CreatedAt': post.CreatedAt,
        'Type': 'Tweet',

        // Headlines
        'Title': '',
        'Url': '',
        'Summary': '',
        'FeedURL': '',
        'PublicationId': 0,

        // Tweet
        'TweetId': 0,
        'Text': '',
        'Username': '',

        // Instagram
        'InstagramId': post.InstagramId,
        'InstgramUsername': username
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
        delete posts.data[i].user;

        // Add to instagram endpoint
        var indexRecord = {
            index: {
                _index: 'instagrams',
                _type: 'instagram',
                _id: posts.data[i].id
            }
        };
        var dataRecord = posts.data[i];
        dataRecord.Username = username;
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
                var error = 'Could not add instagram posts to ES';
                deferred.reject(error);
            }
        }, function(error) {
            deferred.reject(error);
        });
    }, function(error) {
        deferred.reject(error);
    });

    return deferred.promise;
}

// Code for testing the functions above
var message = {
    data: {
        access_token: '43004312.4314d27.3e8c7280a4ec49119e240d8cbaaa89c4',
        username: 'abhiagarwal'
    }
};

processInstagramUser(message.data)
    .then(function(status) {
        console.log('Completed execution for ' + message.data.username);
    }, function(error) {
        console.error(error);
    });