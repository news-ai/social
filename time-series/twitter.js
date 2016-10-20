/*!
 * Adds Twitter post to their user timeseries data.
 */
'use strict';

var Q = require('q');
var elasticsearch = require('elasticsearch');
var moment = require('moment');
var raven = require('raven');

// Instantiate a elasticsearch client
var elasticSearchClient = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search1.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

// Instantiate a sentry client
var sentryClient = new raven.Client('https://bcccc887a5d34c148c8dbeacf51714ef:5b7ee6188efb40c1820789a3e383ce7a@sentry.io/107550');
sentryClient.patchGlobal();

var twitter = exports;

function getTwitterUserTimeseiesFromEs(elasticId) {
    var deferred = Q.defer();

    elasticSearchClient.get({
        index: 'timeseries',
        type: 'twitter',
        id: elasticId
    }).then(function(resp) {
        if (resp.found) {
            deferred.resolve(resp._source.data);
        } else {
            deferred.resolve({});
        }
    }, function(err) {
        deferred.resolve({});
    });

    return deferred.promise;
}

function addDataToElasticsearch(userIndex, newElasticData) {
    var deferred = Q.defer();
    
    var esActions = [];
    var indexRecord = {
        index: {
            _index: 'timeseries',
            _type: 'twitter',
            _id: userIndex.toLowerCase()
        }
    };

    var dataRecord = newElasticData;
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
        deferred.resolve(response);
    });

    return deferred.promise;
}


function addTwitterToUserTimeSeries(twitterProfile) {
    var deferred = Q.defer();

    var username = twitterProfile.screen_name && twitterProfile.screen_name.toLowerCase();
    var today = moment().format('YYYY-MM-DD');
    var userIndex = username + '-' + today;

    var newElasticData = {
        Username: username,
        CreatedAt: today,
        Followers: twitterProfile.followers_count,
        Following: twitterProfile.friends_count,
        Likes: 0,
        Retweets: 0,
        Posts: 0
    };

    getTwitterUserTimeseiesFromEs(userIndex).then(function(data){
        if (data && data.Likes && data.Retweets) {
            newElasticData.Likes = data.Likes;
            newElasticData.Retweets = data.Retweets;
        }

        addDataToElasticsearch(userIndex, newElasticData).then(function(status) {
            deferred.resolve(status);
        }, function (error) {
            sentryClient.captureMessage(error);
            deferred.reject(error);
        });
    }, function (error) {
        sentryClient.captureMessage(error);
        deferred.reject(error);
    });

    return deferred.promise;
}

function addTwitterPostToTimeseries(username, posts) {
    var deferred = Q.defer();

    var today = moment().format('YYYY-MM-DD');
    var userIndex = username + '-' + today;

    var numberOfPosts = posts.length;
    var numberOfLikes = 0;
    var numberofRetweets = 0;

    for (var i = 0; i < posts.length; i++) {
        if (!posts[i].retweeted_status) {
            numberOfLikes += posts[i].favorite_count;
            numberofRetweets += posts[i].retweet_count;
        } else {
            // Remove posts that were retweeted
            numberOfPosts -= 1;
        }
    }

    var newElasticData = {
        Username: username,
        CreatedAt: today,
        Followers: 0,
        Following: 0,
        Likes: numberOfLikes,
        Retweets: numberofRetweets,
        Posts: numberOfPosts
    };

    getTwitterUserTimeseiesFromEs(userIndex).then(function(data) {
        if (data && data.Followers && data.Following) {
            newElasticData.Followers = data.Followers;
            newElasticData.Following = data.Following;
        }

        // Add to elasticsearch
        addDataToElasticsearch(userIndex, newElasticData).then(function(status) {
            deferred.resolve(status);
        }, function (error) {
            sentryClient.captureMessage(error);
            deferred.reject(error);
        });

    }, function (error) {
        console.error(error);
        sentryClient.captureMessage(error);
        deferred.reject(error);
    });

    return deferred.promise;
}

function addTwitterUsersToTimeSeries(userProfiles) {
    var allPromises = [];

    for (var i = 0; i < userProfiles.length; i++) {
        var toExecute = addTwitterToUserTimeSeries(userProfiles[i]);
        allPromises.push(toExecute);
    }

    return Q.all(allPromises);
}

function addTwitterPostsToTimeSeries(twitterPosts) {
    var allPromises = [];

    var today = moment();
    var usernameToTwitterPosts = {};
    for (var i = 0; i < twitterPosts.length; i++) {
        // Filter down for posts only posted today
        var username = twitterPosts[i].user.screen_name.toLowerCase();
        if (today.diff(moment(twitterPosts[i].created_at), 'days') === 0) {
            if (!(username in usernameToTwitterPosts)) {
                usernameToTwitterPosts[username] = []
            }
            usernameToTwitterPosts[username].push(twitterPosts[i]);
        }
    }

    var twitterPostKeys = Object.keys(usernameToTwitterPosts);
    for (var i = 0; i < twitterPostKeys.length; i++) {
        var twitterUsername = twitterPostKeys[i].toLowerCase();
        var toExecute = addTwitterPostToTimeseries(twitterUsername, usernameToTwitterPosts[twitterUsername]);
        allPromises.push(toExecute);
    }

    return Q.all(allPromises);
}

twitter.addTwitterUsersToTimeSeries = addTwitterUsersToTimeSeries;
twitter.addTwitterPostsToTimeSeries = addTwitterPostsToTimeSeries;