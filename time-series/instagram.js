/*!
 * Adds Instagram post to their user timeseries data.
 */
'use strict';

var Q = require('q');
var elasticsearch = require('elasticsearch');
var moment = require('moment');
var Twitter = require('twitter');
var raven = require('raven');

var twitter = require('./twitter');

// Instantiate a elasticsearch client
var elasticSearchClient = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search1.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

var instagram = exports;

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

function addInstagramToUserTimeseries(instagramPost, username) {
    var deferred = Q.defer();

    var today = moment().format('YYYY-MM-DD');
    var userIndex = username + '-' + today;

    var esActions = [];
    indexRecord = {
        index: {
            _index: 'timeseries',
            _type: 'twitter',
            _id: userIndex
        }
    };
    dataRecord = instagramPost;
    esActions.push(indexRecord);
    esActions.push({
        data: dataRecord
    });

    return deferred.promise;
}

instagram.addInstagramToUserTimeseries = addInstagramToUserTimeseries;