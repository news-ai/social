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
var sentryClient = new raven.Client('https://666f957c7dd64957996c1b05675a960a:b942eb7df51d4f8780f55b7d4592a39f@sentry.io/105661');
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
function addToElastic(posts) {
    var allPromises = [];

    var esActions = [];
    // Look through all the instagram data
    for (var i = posts.data.length - 1; i >= 0; i--) {
        var newInstagramPost = posts.data[i];

        var newInstagramPostId = newInstagramPost.id;
        var username = newInstagramPost.username;

        delete newInstagramPost.id;
        delete newInstagramPost.username;

        // Add to instagram endpoint
        var indexRecord = {
            index: {
                _index: 'instagrams',
                _type: 'instagram',
                _id: newInstagramPostId
            }
        };
        var dataRecord = newInstagramPost;
        dataRecord.Username = username;
        esActions.push(indexRecord);
        esActions.push({
            data: dataRecord
        });

        // Add to feeds endpoint
        indexRecord = {
            index: {
                _index: 'feeds',
                _type: 'feed',
                _id: newInstagramPostId
            }
        };
        dataRecord = instagram.formatToFeed(newInstagramPost, username);
        esActions.push(indexRecord);
        esActions.push({
            data: dataRecord
        });
    }

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

    instagram.getInstagramPostsFromEsLastWeek(0, []).then(function(data) {
        instagram.getInstagramPostsFromAPI(data).then(function(instagramPosts) {
            var posts = [];

            for (var i = instagramPosts.length - 1; i >= 0; i--) {
                if (instagramPosts[i].state === 'fulfilled') {
                    instagramPosts[i] = instagramPosts[i].value;

                    var instagramId = [instagramPosts[i].id, instagramPosts[i].owner.id].join('_');
                    var caption = instagramPosts[i].edge_media_to_caption && instagramPosts[i].edge_media_to_caption.edges && instagramPosts[i].edge_media_to_caption.edges.length > 0 && instagramPosts[i].edge_media_to_caption.edges[0] && instagramPosts[i].edge_media_to_caption.edges[0].node && instagramPosts[i].edge_media_to_caption.edges[0].node.text;
                    var tags = caption && caption.match(/#[a-z]+/gi) || [];

                    var post = {
                        'CreatedAt': moment.unix(parseInt(instagramPosts[i].taken_at_timestamp, 10)).format('YYYY-MM-DDTHH:mm:ss'),
                        'Video': instagramPosts[i].video_url || '',
                        'Image': instagramPosts[i].display_url || '',
                        'Location': instagramPosts[i].location && instagramPosts[i].location.name || '',
                        'Coordinates': '',
                        'InstagramId': instagramId || '',
                        'Caption': caption || '',
                        'Likes': instagramPosts[i].edge_media_preview_like && instagramPosts[i].edge_media_preview_like.count || 0,
                        'Comments': instagramPosts[i].edge_media_to_comment && instagramPosts[i].edge_media_to_comment.count || 0,
                        'Link': 'https://www.instagram.com/p/' + instagramPosts[i].shortcode + '/' || '',
                        'Tags': tags || [],
                        'id': instagramId || '',
                        'InstagramHeight': instagramPosts[i].dimensions && instagramPosts[i].dimensions.height || 0,
                        'InstagramWidth': instagramPosts[i].dimensions && instagramPosts[i].dimensions.width || 0,
                    };

                    posts.push(post);
                } else {
                    console.log('deleted posts');
                }
            }

            posts = {
                data: posts
            };

            addToElastic(posts).then(function(status) {
                instagramTimeseries.addInstagramPostsToTimeSeries(posts).then(function(timeseriesStatus) {
                    rp('https://hchk.io/c2b028ef-a86c-4609-8e62-6af6deeed6c4')
                        .then(function(htmlString) {
                            deferred.resolve(timeseriesStatus);
                        })
                        .catch(function(err) {
                            sentryClient.captureMessage(err);
                            console.error(err);
                            deferred.reject(err);
                        });
                }, function(error) {
                    sentryClient.captureMessage(error);
                    deferred.reject(error);
                });
            }, function(error) {
                sentryClient.captureMessage(error);
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
        console.log('Done');
    }, function(error) {
        console.error(error);
    })

    // Run feed code every fifteen minutes
    setInterval(function() {
        console.log('Updating Instagram posts');
        syncIGAndES().then(function(status) {
            console.log('Done');
        }, function(error) {
            sentryClient.captureMessage(error);
            console.error(error);
        })
    }, 60 * 60 * 1000);
}

runUpdates();