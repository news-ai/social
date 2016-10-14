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
var gcloud = require('google-cloud')({
    projectId: 'newsai-1166'
});

// Instantiate a elasticsearch client
var elasticSearchClient = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search1.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

// Instantiate a sentry client
var sentryClient = new raven.Client('https://a026de7b0e4b40448b769ad8d17c8a90:d7fd5ae279134c51bc10e37c5485b93f@sentry.io/106015');
sentryClient.patchGlobal();

function getInstagramPageFromEs(offset) {
    var deferred = Q.defer();

    elasticSearchClient.search({
        index: 'instagrams',
        type: 'user',
        body: {
            "query": {
                "filtered": {
                    "query": {
                        "match_all": {}
                    }
                }
            },
            "size": 100,
            "from": 100 * offset
        }
    }).then(function(resp) {
        var hits = resp.hits.hits;
        deferred.resolve(hits);
    }, function(err) {
        console.trace(err.message);
    });

    return deferred.promise;
}

function getInstagramProfiles(offset, allData) {
    var deferred = Q.defer();

    getInstagramPageFromEs(offset).then(function(data) {
        if (data.length === 0) {
            deferred.resolve(allData);
        } else {
            var newData = allData.concat(data);
            deferred.resolve(getInstagramProfiles(offset + 1, newData));
        }
    });

    return deferred.promise;
}

function getInstagramFromProfileLink(profileLink) {
    var deferred = Q.defer();

    request(profileLink + '?__a=1', function(error, response, body) {
        if (!error && response.statusCode == 200) {
            var instagramProfile = JSON.parse(body);
            var instagramUser = instagramProfile.user;
            deferred.resolve(instagramUser);
        } else {
            sentryClient.captureMessage(body);
            deferred.reject(new Error(body));
        }
    });

    return deferred.promise;
}

function getInstagramProfilesFromAPI(data) {
    var allPromises = [];
    for (var i = data.length - 1; i >= 0; i--) {
        var url = 'https://www.instagram.com/' + data[i]._id + '/';
        var toExecute = getInstagramFromProfileLink(url);
        allPromises.push(toExecute);
    }
    return Q.allSettled(allPromises);
}

function syncIGAndES() {
    var deferred = Q.defer();

    getInstagramProfiles(0, []).then(function(data) {
        console.log(data.length);
        getInstagramProfilesFromAPI(data).then(function(instagramProfiles) {
            console.log(instagramProfiles.length);

            console.log(instagramProfiles[0].value.media.nodes)
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
    }, 6 * 60 * 60 * 1000);
}

runUpdates();