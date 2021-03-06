/*!
 * Syncs single Instagram posts from Instagram API to ES
 */
'use strict';

var Q = require('q');
var rp = require('request-promise');
var request = require('requestretry');
var elasticsearch = require('elasticsearch');
var moment = require('moment');
var raven = require('raven');

var instagram = require('./instagram');
var instagramTimeseries = require('../../time-series/instagram');

// Instantiate a elasticsearch client
var elasticSearchClient = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

// Instantiate a sentry client
var sentryClient = new raven.Client('https://a026de7b0e4b40448b769ad8d17c8a90:d7fd5ae279134c51bc10e37c5485b93f@sentry.io/106015');
sentryClient.patchGlobal();

function bulkAddEleastic(esActions) {
    var deferred = Q.defer();

    elasticSearchClient.bulk({
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

// Add these instagram posts to ElasticSearch
// username here is the base parent username.
// Not just a username of any user.
function addToElastic(profiles) {
    var deferred = Q.defer();

    var esActions = [];

    for (var i = profiles.data.length - 1; i >= 0; i--) {
        var indexRecord = {
            index: {
                _index: 'instagrams',
                _type: 'user',
                _id: profiles.data[i].username
            }
        };

        var user = {
            'username': profiles.data[i].username || '',
            'bio': profiles.data[i].biography || '',
            'website': profiles.data[i].external_url || '',
            'profile_picture': profiles.data[i].profile_pic_url || '',
            'full_name': profiles.data[i].full_name || '',
            'counts': {
                'media': profiles.data[i].media.count || 0,
                'followed_by': profiles.data[i].followed_by && profiles.data[i].followed_by.count || 0,
                'follows': profiles.data[i].follows && profiles.data[i].follows.count || 0
            },
            'id': profiles.data[i].id || ''
        };


        var dataRecord = user;
        dataRecord.Username = profiles.data[i].username;
        esActions.push(indexRecord);
        esActions.push({
            data: dataRecord
        });
    }

    var allPromises = [];

    if (esActions.length > 0) {
        // Has to be an even number
        var i, j, temp, chunk = 24;
        for (i = 0, j = esActions.length; i < j; i += chunk) {
            temp = esActions.slice(i, i + chunk);

            var toExecute = bulkAddEleastic(temp);
            allPromises.push(toExecute);
        }
    } else {
        var error = 'Nothing was added to ES for username ' + username;
        sentryClient.captureMessage(error);
        deferred.reject(error);
    }

    return Q.allSettled(allPromises);
}

function syncIGAndES() {
    var deferred = Q.defer();

    instagram.getInstagramProfiles(0, []).then(function(data) {
        instagram.getInstagramProfilesFromAPI(data).then(function(instagramProfiles) {
            var profiles = [];

            for (var i = instagramProfiles.length - 1; i >= 0; i--) {
                if (instagramProfiles[i].state === 'fulfilled') {
                    instagramProfiles[i] = instagramProfiles[i].value;
                    profiles.push(instagramProfiles[i]);
                } else {
                    console.log('deleted profile');
                }
            }

            profiles = {
                data: profiles
            };

            addToElastic(profiles).then(function(status) {
                // Sync data to instagram TimeSeries
                instagramTimeseries.addInstagramUsersToTimeSeries(profiles).then(function(esStatus) {
                    rp('https://hchk.io/31a96c67-555f-47a0-b6e3-6ed4dc3980cf')
                        .then(function(htmlString) {
                            deferred.resolve(esStatus);
                        })
                        .catch(function(error) {
                            sentryClient.captureMessage(error);
                            console.error(error);
                            deferred.reject(error);
                        });
                }, function(error) {
                    sentryClient.captureMessage(error);
                    console.error(error);
                    deferred.reject(error);
                });
            }, function(error) {
                sentryClient.captureMessage(error);
                console.error(error);
                deferred.reject(error);
            });
        }, function(error) {
            sentryClient.captureMessage(error);
            console.error(error);
            deferred.reject(error);
        });
    }, function(error) {
        sentryClient.captureMessage(error);
        console.error(error);
        deferred.reject(error);
    });

    return deferred.promise;
}

function runUpdates() {
    // Run one initially -- mostly for when testing
    console.log('Beginning run');
    syncIGAndES().then(function(status) {
        console.log('Completed execution');
    }, function(error) {
        sentryClient.captureMessage(error);
        console.error(error);
    })

    // Run feed code every fifteen minutes
    setInterval(function() {
        console.log('Updating Instagram profiles');
        syncIGAndES().then(function(status) {
            console.log('Completed execution');
        }, function(error) {
            sentryClient.captureMessage(error);
            console.error(error);
        })
    }, 6 * 60 * 60 * 1000);
}

runUpdates();