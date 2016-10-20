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
        console.trace(err.message);
    });

    return deferred.promise;
}

function addInstagramToUserTimeseries(instagramProfile) {
    var deferred = Q.defer();

    var username = instagramProfile.username.toLowerCase();
    var today = moment().format('YYYY-MM-DD');
    var userIndex = username + '-' + today;

    var newElasticData = {
        Username: username,
        CreatedAt: today,
        Followers: instagramProfile.followed_by && instagramProfile.followed_by.count || 0,
        Following: instagramProfile.follows && instagramProfile.follows.count || 0
    };

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

    getInstagramUserTimeseiesFromEs(userIndex).then(function(data) { 
        console.log(data);
    }, function (error) {
        console.error(error);
    });

    return deferred.promise;
}

function addInstagramUsersToTimeSeries(userProfiles) {
    var allPromises = [];

    for (var i = 0; i < userProfiles.data.length; i++) {
        var toExecute = addInstagramToUserTimeseries(userProfiles.data[i]);
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