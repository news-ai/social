'use strict';

var rp = require('request-promise');
var Stream = require('user-stream');
var raven = require('raven');

// Instantiate a sentry client
var sentryClient = new raven.Client('https://9af56ecaeca547abb4aa9f1bed0626d9:8146296e132a4dd2808b0babdaebfc4c@sentry.io/103129');
sentryClient.patchGlobal();

// Initialize Twitter client and Twitter stream
var stream = new Stream({
    consumer_key: 'nu83S4GaW4vrsN6gPoTbSvuMy',
    consumer_secret: 't86zlLxN7mjwHu9OMflX806StaqSFWfLMTOiiFLmOuwI5kUFFE',
    access_token_key: '758002735547609088-bPZJ1mO8nPfHq52FquOh0tsaWa6Fc28',
    access_token_secret: 'NIYOhbJZSFzKNRJGVdtPlzMnzKet9bHdwH08ghw9TmzWr'
});
stream.stream();

function processTweet(tweet) {
    var deferred = Q.defer();

    if (tweet && tweet.user && tweet.user.id) {
        findUsernameFromTwitterId(tweet.user.id).then(function(username) {
            addTweetToEs(tweet, username).then(function(status) {
                if (status) {
                    deferred.resolve(true);
                } else {
                    var error = 'Elasticsearch add failed';
                    console.error(error);
                    sentryClient.captureMessage(error);
                    deferred.reject(error);
                }
            }, function(error) {
                console.error(error);
                sentryClient.captureMessage(error);
                deferred.reject(error);
            });
        }, function(error) {
            console.error(error);
            sentryClient.captureMessage(error);
            deferred.reject(error);
        });
    }

    return deferred.promise;
}

/**
 * Events:
 * - data
 * - garbage
 * - close
 * - error
 * - connected
 * - heartbeat
 */

// Error checking
stream.on('connected', function(error) {
    console.log('Connected to stream');
});

// Incoming tweet for a particular user - add to ElasticSearch
stream.on('data', function(tweet) {
    console.log(tweet.id);
    if (!tweet.friends) {
        processTweet(tweet).then(function(response) {
            rp('https://hchk.io/73a12c55-c81f-4f0e-b0fe-cc0e26c18cd7')
                .then(function(htmlString) {
                    console.log(response);
                })
                .catch(function(err) {
                    console.error(err);
                });
        }, function(error) {
            console.error(error);
            sentryClient.captureMessage(error);
        });
    }
});

// Garbage
// Unsure why it places it as garbage, but process it anyways.
// If it throws an error who cares.
stream.on('garbage', function(tweet) {
    console.log(tweet.id);
    if (!tweet.friends && tweet.id) {
        processTweet(tweet).then(function(response) {
            rp('https://hchk.io/73a12c55-c81f-4f0e-b0fe-cc0e26c18cd7')
                .then(function(htmlString) {
                    console.log(response);
                })
                .catch(function(err) {
                    console.error(err);
                });
        }, function(error) {
            console.error(error);
            sentryClient.captureMessage(error);
        });
    }
});

// Error checking
stream.on('error', function(error) {
    // Restart stream
    console.log('[ERROR]: ' + error);
    sentryClient.captureMessage('[ERROR]: ' + error);
});

// Error checking - restart stream
stream.on('close', function(error) {
    // Restart stream
    console.log('[STREAM CLOSED]: ' + error);
    sentryClient.captureMessage('[STREAM CLOSED]: ' + error);
    stream.stream();
});