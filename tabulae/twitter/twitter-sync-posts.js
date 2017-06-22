/*!
 * Syncs single Twitter posts from Twitter API to ES
 */
'use strict';

var rp = require('request-promise');
var Twitter = require('twitter');
var raven = require('raven');

// Instantiate a twitter client
var twitterClient = new Twitter({
    consumer_key: 'nu83S4GaW4vrsN6gPoTbSvuMy',
    consumer_secret: 't86zlLxN7mjwHu9OMflX806StaqSFWfLMTOiiFLmOuwI5kUFFE',
    access_token_key: '758002735547609088-bPZJ1mO8nPfHq52FquOh0tsaWa6Fc28',
    access_token_secret: 'NIYOhbJZSFzKNRJGVdtPlzMnzKet9bHdwH08ghw9TmzWr'
});

var syncPostsTwitter = require('../../twitter/twitter-sync-posts');

// Instantiate a sentry client
var sentryClient = new raven.Client('https://27c00cc5e3ad42c2982d394811eb7633:f2dbda55263b4d40a603442926782cf4@sentry.io/106930');
sentryClient.patchGlobal();

function runUpdates() {
    // Run one initially -- mostly for when testing
    console.log('Beginning run');
    syncPostsTwitter.syncTwitterAndES(twitterClient, sentryClient, 'tweet', 'feed').then(function(status) {
        rp('https://hchk.io/a58c62df-5369-4476-b2f2-6c309949a75a')
            .then(function(htmlString) {
                console.log('Finished')
            })
            .catch(function(error) {
                console.error(error);
                sentryClient.captureMessage(error);
            });
    }, function(error) {
        sentryClient.captureMessage(error);
        console.error(error);
    })

    // Run feed code every fifteen minutes
    setInterval(function() {
        console.log('Updating Twitter posts');
        syncPostsTwitter.syncTwitterAndES(twitterClient, sentryClient, 'tweet', 'feed').then(function(status) {
            rp('https://hchk.io/a58c62df-5369-4476-b2f2-6c309949a75a')
                .then(function(htmlString) {
                    console.log('Finished')
                })
                .catch(function(error) {
                    console.error(error);
                    sentryClient.captureMessage(error);
                });
        }, function(error) {
            sentryClient.captureMessage(error);
            console.error(error);
        })
    }, 15 * 60 * 1000);
}

runUpdates();