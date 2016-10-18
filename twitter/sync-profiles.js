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
