/*!
 * Syncs single Twitter profiles from Twitter API to ES
 */
'use strict';

var Q = require('q');
var elasticsearch = require('elasticsearch');
var moment = require('moment');
var Twitter = require('twitter');
var raven = require('raven');

var twitter = require('./twitter');
var twitterTimeseries = require('../time-series/twitter');

// Instantiate a elasticsearch client
var elasticSearchClient = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search1.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

// Instantiate a twitter client
var twitterClient = new Twitter({
    consumer_key: 'nu83S4GaW4vrsN6gPoTbSvuMy',
    consumer_secret: 't86zlLxN7mjwHu9OMflX806StaqSFWfLMTOiiFLmOuwI5kUFFE',
    access_token_key: '758002735547609088-bPZJ1mO8nPfHq52FquOh0tsaWa6Fc28',
    access_token_secret: 'NIYOhbJZSFzKNRJGVdtPlzMnzKet9bHdwH08ghw9TmzWr'
});

// Instantiate a sentry client
var sentryClient = new raven.Client('https://5099d6dcd0334f69ba7431249da1dfd2:957fc47e9f19472db28dcb9e7008742b@sentry.io/106942');
sentryClient.patchGlobal();

function addToElastic(userProfiles) {
    var deferred = Q.defer();

    var esActions = [];

    for (var i = 0; i < userProfiles.length; i++) {
        // Add user to ElasticSearch as well
        var indexRecord = {
            index: {
                _index: 'tweets',
                _type: 'user',
                _id: userProfiles[i].screen_name
            }
        };
        var dataRecord = userProfiles[i];
        dataRecord.Username = userProfiles[i].screen_name;
        delete dataRecord.status;
        esActions.push(indexRecord);
        esActions.push({
            data: dataRecord
        });
    }

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

// Profiles from Twitter
function getTwitterProfilesFromUsernames(twitterUsernames) {
    var deferred = Q.defer();

    twitterClient.get('users/lookup', {
        screen_name: twitterUsernames.join()
    }, function(error, twitterProfiles, response) {
        if (!error) {
            deferred.resolve(twitterProfiles);
        } else {
            console.log(response);
            console.error(error);
            sentryClient.captureMessage(error);
            deferred.reject(new Error(error));
        }
    });

    return deferred.promise;
}

function groupTwitterProfilesByUsernames(twitterUsernames) {
    var allPromises = [];

    var i, j, temp, chunk = 99;
    for (i = 0, j = twitterUsernames.length; i < j; i += chunk) {
        temp = twitterUsernames.slice(i, i + chunk);
        var toExecute = getTwitterProfilesFromUsernames(temp);
        allPromises.push(toExecute);
    }

    return Q.all(allPromises);
}

function syncTwitterAndES() {
    var deferred = Q.defer();

    // Get tweets from ES
    twitter.getTwitterProfilesFromEs(0, []).then(function(data) {
        console.log(data.length);
        var twitterUsernames = [];

        for (var i = 0; i < data.length; i++) {
            twitterUsernames.push(data[i]._id);
        }

        groupTwitterProfilesByUsernames(twitterUsernames).then(function (twitterProfiles) {
            var allProfiles = [].concat.apply([], twitterProfiles);

            // Add the data to elasticsearch
            addToElastic(allProfiles).then(function(status) {
                console.log('Number of Twitter profiles in ES: ' + allProfiles.length);
                twitterTimeseries.addTwitterUsersToTimeSeries(allProfiles).then(function (status) {
                    deferred.resolve(status);
                }, function (error) {
                    sentryClient.captureMessage(error);
                    deferred.reject(error);
                })
            }, function(error) {
                sentryClient.captureMessage(error);
                deferred.reject(error);
            });
        });

    }, function (error) {
        sentryClient.captureMessage(error);
        console.error(error);
        deferred.reject(error);
    });

    return deferred.promise;
}

function runUpdates() {
    // Run one initially -- mostly for when testing
    console.log('Beginning run');
    syncTwitterAndES().then(function(status) {
        for (var i = 0; i < status.length; i++) {
            if (status[i].errors) {
                console.log(status[i].items);
            }
        }
    }, function(error) {
        console.error(error);
    })

    // Run feed code every fifteen minutes
    setInterval(function() {
        console.log('Updating Twitter posts');
        syncTwitterAndES().then(function(status) {
            console.log(status);
        }, function(error) {
            sentryClient.captureMessage(error);
            console.error(error);
        })
    }, 15 * 60 * 1000);
}

runUpdates();