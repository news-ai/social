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

function addTwitterToUserTimeseries(twitterPost, username) {
    var deferred = Q.defer();

    var today = moment().format('YYYY-MM-DD');
    var userIndex = username + '-' + today;

    getTwitterUserTimeseiesFromEs(userIndex).then(function (elasticData){

        var newElasticData = {
            Username: username,
            CreatedAt: today
        }

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
    }, function (error) {
        console.log(error);
    });

    return deferred.promise;
}

twitter.addTwitterToUserTimeseries = addTwitterToUserTimeseries;