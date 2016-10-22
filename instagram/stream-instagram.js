/*!
 * Looks for when people are posting new Instagram posts
 */
'use strict';

var Q = require('q');
var rp = require('request-promise');
var request = require('requestretry');
var elasticsearch = require('elasticsearch');
var moment = require('moment');
var raven = require('raven');

var instagram = require('./instagram');

// Instantiate a elasticsearch client
var elasticSearchClient = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search1.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

// Initialize Google Cloud
var topicName = 'process-instagram-feed';

// Instantiate a sentry client
var sentryClient = new raven.Client('https://a026de7b0e4b40448b769ad8d17c8a90:d7fd5ae279134c51bc10e37c5485b93f@sentry.io/106015');
sentryClient.patchGlobal();

function sendInstagramProfileToPubsub(data) {
    var allPromises = [];
    for (var i = data.length - 1; i >= 0; i--) {
        console.log('Starting execution for ' + data[i].username);
        var toExecute = instagram.addFeedToPubSub(topicName, data[i]);
        allPromises.push(toExecute);
    }
    return Q.allSettled(allPromises);
}

function syncIGAndES() {
    var deferred = Q.defer();

    instagram.getInstagramProfiles(0, []).then(function(data) {
        var allData = [];
        for (var i = 0; i < data.length; i++) {
            var currentData = {
                username: data[i]._id,
                access_token: ''
            };
            allData.push(currentData);
        }
        sendInstagramProfileToPubsub(allData).then(function(status) {
            rp('https://hchk.io/92155727-1536-47d6-b3df-5eb558d5f561')
                .then(function(htmlString) {
                    deferred.resolve(status);
                })
                .catch(function(err) {
                    console.error(err);
                    sentryClient.captureMessage(err);
                    deferred.reject(err);
                });
        }, function(error) {
            console.error(error);
            sentryClient.captureMessage(error);
            deferred.reject(error);
        });
    });

    return deferred.promise;
}

function runUpdates() {
    // Run one initially -- mostly for when testing
    console.log('Beginning run');
    syncIGAndES().then(function(status) {
        console.log(status);
    }, function(error) {
        console.error(error);
    })

    // Run feed code every fifteen minutes
    setInterval(function() {
        console.log('Getting data from Instagram profiles');
        syncIGAndES().then(function(status) {
            console.log('Completed execution');
        }, function(error) {
            sentryClient.captureMessage(error);
            console.error(error);
        })
    }, 60 * 60 * 1000);
}

runUpdates();