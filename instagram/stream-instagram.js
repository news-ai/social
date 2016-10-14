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

// Instantiate a sentry client
var sentryClient = new raven.Client('https://a026de7b0e4b40448b769ad8d17c8a90:d7fd5ae279134c51bc10e37c5485b93f@sentry.io/106015');
sentryClient.patchGlobal();

function syncIGAndES() {
    var deferred = Q.defer();

    instagram.getInstagramProfiles(0, []).then(function(data) {
        console.log(data.length);
        instagram.getInstagramProfilesFromAPI(data).then(function(instagramProfiles) {
            console.log(instagramProfiles.length);

            var posts = [];

            for (var i = 0; i < instagramProfiles.length; i++) {
                if (instagramProfiles[i].state === 'fulfilled') {
                    var username = instagramProfiles[i].value.username;
                    for (var x = 0; x < instagramProfiles[i].value.media.nodes.length; x++) {
                        console.log(instagramProfiles[i].value.media.nodes[x]);
                    }
                }
            }
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

    // // Run feed code every fifteen minutes
    // setInterval(function() {
    //     console.log('Getting data from Instagram profiles');
    //     syncIGAndES().then(function(status) {
    //         console.log('Completed execution');
    //     }, function(error) {
    //         sentryClient.captureMessage(error);
    //         console.error(error);
    //     })
    // }, 6 * 60 * 60 * 1000);
}

runUpdates();