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
        var hits = resp.hits.hits;
        deferred.resolve(hits);
    }, function(err) {
        console.trace(err.message);
    });

    return deferred.promise;
}

function addTwitterToUserTimeSeries(twitterProfile) {
    var deferred = Q.defer();

    var username = twitterProfile.screen_name.toLowerCase();
    var today = moment().format('YYYY-MM-DD');
    var userIndex = username + '-' + today;

    // getTwitterUserTimeseiesFromEs(userIndex).then(function (elasticData){
    //     var newElasticData = {
    //         Username: username,
    //         CreatedAt: today,
    //         Followers: twitterProfile.followers_count,
    //         Following: twitterProfile.friends_count
    //     };

    //     var esActions = [];
    //     var indexRecord = {
    //         index: {
    //             _index: 'timeseries',
    //             _type: 'twitter',
    //             _id: userIndex
    //         }
    //     };
    //     var dataRecord = newElasticData;
    //     esActions.push(indexRecord);
    //     esActions.push({
    //         data: dataRecord
    //     });
    // }, function (error) {
    //     console.log(error);
    // });

    var newElasticData = {
        Username: username,
        CreatedAt: today,
        Followers: twitterProfile.followers_count,
        Following: twitterProfile.friends_count
    };

    var esActions = [];
    var indexRecord = {
        index: {
            _index: 'timeseries',
            _type: 'twitter',
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

function addTwitterUsersToTimeSeries(userProfiles) {
    var allPromises = [];

    for (var i = 0; i < userProfiles.length; i++) {
        var toExecute = addTwitterToUserTimeSeries(userProfiles[i]);
        allPromises.push(toExecute);
    }

    return Q.all(allPromises);
}

twitter.addTwitterUsersToTimeSeries = addTwitterUsersToTimeSeries;