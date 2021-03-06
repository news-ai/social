'use strict';

var moment = require('moment');
var rp = require('request-promise');
var feedparser = require('feedparser-promised');
var elasticsearch = require('elasticsearch');
var Q = require('q');
var raven = require('raven');
var gcloud = require('google-cloud')({
    projectId: 'newsai-1166'
});


// Instantiate a elasticsearch client
var client = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

// Initialize Google Cloud
var topicName = 'process-rss-feed';
var subscriptionName = 'node-rss';
var pubsub = gcloud.pubsub();

// Instantiate a sentry client
var sentryClient = new raven.Client('https://4db5dd699d4a4267ab6f56fa97a9ee5c:9240e5b57b864de58f514b6d40e7e5a7@sentry.io/103131');
sentryClient.patchGlobal();

// Get a Google Cloud topic
function getTopic(cb) {
    pubsub.createTopic(topicName, function(err, topic) {
        // topic already exists.
        if (err && err.code === 409) {
            return cb(null, pubsub.topic(topicName));
        }
        return cb(err, topic);
    });
}

// Extract all articles from a RSS URL
function getFeedFromUrl(url) {
    var deferred = Q.defer();

    feedparser.parse(url).then(function(items) {
        var contents = [];

        items.forEach(function(item) {
            if (!item.pubDate) {
                item.pubDate = moment().format('YYYY-MM-DDTHH:mm:ss');
            } else {
                item.pubDate = moment(item.pubDate).format('YYYY-MM-DDTHH:mm:ss');
            }
            if (!item.title) {
                item.title = "";
            }
            if (!item.author) {
                item.author = "";
            }
            if (!item.link) {
                item.link = "";
            }
            if (!item.categories) {
                item.categories = [];
            }
            if (!item.summary) {
                item.summary = "";
            }
            if (!url) {
                url = "";
            }
            var content = {
                Title: item.title,
                Author: item.author,
                Url: item.link,
                Categories: item.categories,
                PublishDate: item.pubDate,
                Summary: item.summary,
                FeedURL: url
            }
            contents.push(content);
        });
        deferred.resolve(contents);
    }).catch(function(error) {
        deferred.reject(new Error(error));
        sentryClient.captureMessage(error);
        throw new Error(error);
    });

    return deferred.promise;
}

function formatToFeed(headline, publicationId) {
    return {
        'CreatedAt': headline.PublishDate,
        'Type': 'Headline',

        // Headlines
        'Title': headline.Title,
        'Url': headline.Url,
        'Summary': headline.Summary,
        'FeedURL': headline.FeedURL,
        'PublicationId': publicationId,

        // Tweet
        'TweetId': 0,
        'Text': '',
        'Username': ''
    };
}

function bulkAddEleastic(esActions) {
    var deferred = Q.defer();

    client.bulk({
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

function addToElastic(publicationId, content) {
    var allPromises = [];

    if (Object.prototype.toString.call(publicationId) === '[object String]') {
        publicationId = parseInt(publicationId, 10);
    }

    // If content length is zero then resolve right away
    if (content.length > 0) {
        var esActions = [];
        for (var i = content.length - 1; i >= 0; i--) {
            // If the feed doesn't have a URL field
            var idField = "";
            if (content[i].Url !== '') {
                idField = content[i].Url;
            } else {
                idField = content[i].FeedURL + content[i].PublishDate + content[i].Title.replace(' ', '');
            }

            var indexRecord = {
                index: {
                    _index: 'headlines',
                    _type: 'headline',
                    _id: idField
                }
            };
            var dataRecord = content[i];
            dataRecord.PublicationId = publicationId;
            esActions.push(indexRecord);
            esActions.push({
                data: dataRecord
            });

            indexRecord = {
                index: {
                    _index: 'feeds',
                    _type: 'feed',
                    _id: idField
                }
            };
            dataRecord = formatToFeed(content[i], publicationId);
            esActions.push(indexRecord);
            esActions.push({
                data: dataRecord
            });
        }

        // Has to be an even number
       var i, j, temp, chunk = 24;
       for (i = 0, j = esActions.length; i < j; i += chunk) {
           temp = esActions.slice(i, i + chunk);

           var toExecute = bulkAddEleastic(temp);
           allPromises.push(toExecute);
       }
        
    }

    return Q.allSettled(allPromises);
}

function getContent(data) {
    var deferred = Q.defer();

    getFeedFromUrl(data.url).then(function(content) {
        addToElastic(data.publicationId, content).then(function(status) {
            if (status) {
                deferred.resolve(true);
            } else {
                var error = 'Elasticsearch add failed';
                console.error(error);
                sentryClient.captureMessage(error);
                deferred.resolve(false);
                throw new Error(error);
            }
        }, function(error) {
            console.error(error);
            sentryClient.captureMessage(error);
            deferred.resolve(false);
            throw new Error(error);
        });
    }, function(error) {
        console.error(error);
        sentryClient.captureMessage(error);
        deferred.reject(new Error(error));
        throw new Error(error);
    });

    return deferred.promise;
}

function subscribe(cb) {
    var subscription;

    // Event handlers
    function handleMessage(message) {
        cb(null, message);
    }

    function handleError(err) {
        console.error(err);
        sentryClient.captureMessage(err);
    }

    getTopic(function(err, topic) {
        if (err) {
            sentryClient.captureMessage(err);
            return cb(err);
        }

        topic.subscribe(subscriptionName, {
            autoAck: true,
            reuseExisting: true
        }, function(err, sub) {
            if (err) {
                return cb(err);
            }

            subscription = sub;

            // Listen to and handle message and error events
            subscription.on('message', handleMessage);
            subscription.on('error', handleError);

            console.log('Listening to ' + topicName +
                ' with subscription ' + subscriptionName);
        });
    });

    // Subscription cancellation function
    return function() {
        if (subscription) {
            // Remove event listeners
            subscription.removeListener('message', handleMessage);
            subscription.removeListener('error', handleError);
            subscription = undefined;
        }
    };
}

subscribe(function(err, message) {
    // Any errors received are considered fatal.
    if (err) {
        console.error(err);
        sentryClient.captureMessage(err);
        throw err;
    }
    console.log('Received request to process rss feed ' + message.data.url);
    getContent(message.data)
        .then(function(status) {
            rp('https://hchk.io/8c3456ca-6b17-412c-80fb-d407d5f32b45')
                .then(function(htmlString) {
                    console.log('Completed execution for ' + message.data.url);
                })
                .catch(function(err) {
                    console.error(err);
                });
        }, function(error) {
            console.error(error);
            sentryClient.captureMessage(error);
        });
});

// var message = {
//     data: {
//         'publicationId': 5166990612234240,
//         'url': 'http://feeds.feedburner.com/TheEzraKleinShow'
//     }
// }

// getContent(message.data)
//         .then(function(status) {
//             rp('https://hchk.io/8c3456ca-6b17-412c-80fb-d407d5f32b45')
//                 .then(function (htmlString) {
//                     console.log('Completed execution for ' + message.data.url);
//                 })
//                 .catch(function (err) {
//                     console.error(err);
//                 });
//         }, function(error) {
//             console.error(error);
//             sentryClient.captureMessage(error);
//         });