/*!
 * Adds Instagram post to their user timeseries data.
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
var sentryClient = new raven.Client('https://01a37de7973341628dc29671ba8b7933:382639f851304ae98f55960f242e192b@sentry.io/107549');
sentryClient.patchGlobal();

var instagram = exports;

function getInstagramUserTimeseiesFromEs(elasticId) {
    var deferred = Q.defer();

    elasticSearchClient.get({
        index: 'timeseries',
        type: 'instagram',
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

function addInstagramToUserTimeseries(userIndex, newElasticData) {
    var deferred = Q.defer();

    var esActions = [];
    var indexRecord = {
        index: {
            _index: 'timeseries',
            _type: 'instagram',
            _id: userIndex
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

function addInstagramPostToTimeseries(username, posts) {
    var deferred = Q.defer();

    var today = moment().format('YYYY-MM-DD');
    var userIndex = username + '-' + today;

    var numberOfPosts = posts.length;
    var numberOfLikes = 0;
    var numberofComments = 0;

    for (var i = 0; i < posts.length; i++) {
        numberOfLikes += posts[i].Likes;
        numberofComments += posts[i].Comments;
    }

    var newElasticData = {
        Username: username,
        CreatedAt: today,
        Likes: numberOfLikes,
        Comments: numberofComments,
        Posts: numberOfPosts,
        Followers: 0,
        Following: 0
    };

    getInstagramUserTimeseiesFromEs(userIndex).then(function(data) {
        if (data && data.Followers && data.Following) {
            newElasticData.Followers = data.Followers;
            newElasticData.Following = data.Following;
        }

        addInstagramToUserTimeseries(userIndex, newElasticData).then(function (status) {
            deferred.resolve(status);
        }, function (error) {
            console.error(error);
            sentryClient.captureMessage(error);
            deferred.reject(error);
        });
    }, function (error) {
        console.error(error);
    });

    return deferred.promise;
}

function addInstagramUserToExistingTimeseries(userIndex, newElasticData) {
    var deferred = Q.defer();

    getInstagramUserTimeseiesFromEs(userIndex).then(function(data) {
        if (data && data.Followers && data.Following) {
            newElasticData.Followers = data.Followers;
            newElasticData.Following = data.Following;
        }

        addInstagramToUserTimeseries(userIndex, newElasticData).then(function (status) {
            deferred.resolve(status);
        }, function (error) {
            console.error(error);
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

function addInstagramUsersToTimeSeries(userProfiles) {
    var allPromises = [];

    for (var i = 0; i < userProfiles.data.length; i++) {
        var instagramProfile = userProfiles.data[i];

        var username = instagramProfile.username.toLowerCase();
        var today = moment().format('YYYY-MM-DD');
        var userIndex = username + '-' + today;

        var newElasticData = {
            Username: username,
            CreatedAt: today,
            Followers: instagramProfile.followed_by && instagramProfile.followed_by.count || 0,
            Following: instagramProfile.follows && instagramProfile.follows.count || 0,
            Likes: 0,
            Comments: 0,
            Posts: 0
        };

        var toExecute = addInstagramUserToExistingTimeseries(userIndex, newElasticData);
        allPromises.push(toExecute);
    }

    return Q.all(allPromises);
}

function addInstagramPostsToTimeSeries(instagramPosts) {
    var allPromises = [];

    instagramPosts = instagramPosts.data;

    var today = moment();
    var usernameToInstagramPosts = {};
    for (var i = 0; i < instagramPosts.length; i++) {
        // Filter down for posts only posted today
        if (today.diff(moment(instagramPosts[i].CreatedAt), 'days') === 0) {
            if (!(instagramPosts[i].Username in usernameToInstagramPosts)) {
                usernameToInstagramPosts[instagramPosts[i].Username] = []
            }
            usernameToInstagramPosts[instagramPosts[i].Username].push(instagramPosts[i]);
        }
    }

    var instagramPostKeys = Object.keys(usernameToInstagramPosts);
    for (var i = 0; i < instagramPostKeys.length; i++) {
        var instagramUsername = instagramPostKeys[i];
        var toExecute = addInstagramPostToTimeseries(instagramUsername, usernameToInstagramPosts[instagramUsername]);
        allPromises.push(toExecute);
    }

    return Q.all(allPromises);
}

instagram.addInstagramUsersToTimeSeries = addInstagramUsersToTimeSeries;
instagram.addInstagramPostsToTimeSeries = addInstagramPostsToTimeSeries;