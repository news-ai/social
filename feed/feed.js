'use strict';

var Q = require('q');

// Instantiate a datastore client
var datastore = require('@google-cloud/datastore')({
    projectId: 'newsai-1166'
});


function runFeeds() {
    console.log('Running')
    setInterval(function() {
        console.log("I am doing my 5 minutes check");
        // do your stuff here
    }, 15 * 60 * 1000);
}

runFeeds();