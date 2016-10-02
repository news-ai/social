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
        'InstagramId': post.InstagramId
    };
}

// Add these instagram posts to ElasticSearch
// username here is the base parent username.
// Not just a username of any user.
function addToElastic(username, posts) {
    var deferred = Q.defer();

    return deferred.promise;
}

function getInstagramFromUsername(access_token, username) {
    var deferred = Q.defer();

    request('https://api.instagram.com/v1/users/self/media/recent/?access_token=' + access_token, function(error, response, body) {
        if (!error && response.statusCode == 200) {
            var instagramMedia = JSON.parse(body);
            var instagramUser = instagramMedia.data[0].user;
            var instagramData = [];

            // Look through all the instagram data
            for (var i = instagramMedia.data.length - 1; i >= 0; i--) {
                delete instagramMedia.data[i].user;
                instagramMedia.data[i].Username = username;
                instagramData.push(instagramMedia.data[i]);
            }

            deferred.resolve(instagramData);
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
        console.log(posts);

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