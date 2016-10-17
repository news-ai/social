'use strict';

var elasticsearch = require('elasticsearch');
var rp = require('request-promise');
var moment = require('moment');
var Q = require('q');
var Twitter = require('twitter');
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

// Initialize Google Cloud
var topicName = 'process-twitter-feed';
var subscriptionName = 'node-new-user-twitter';
var pubsub = gcloud.pubsub();

// Instantiate a sentry client
var sentryClient = new raven.Client('https://9fd2d37a95dc472496b018dd15be1369:195a98802d4f4440852aea96ebb65d2b@sentry.io/103128');
sentryClient.patchGlobal();

// Instantiate a twitter client
var twitterClient = new Twitter({
    consumer_key: 'nu83S4GaW4vrsN6gPoTbSvuMy',
    consumer_secret: 't86zlLxN7mjwHu9OMflX806StaqSFWfLMTOiiFLmOuwI5kUFFE',
    access_token_key: '758002735547609088-bPZJ1mO8nPfHq52FquOh0tsaWa6Fc28',
    access_token_secret: 'NIYOhbJZSFzKNRJGVdtPlzMnzKet9bHdwH08ghw9TmzWr'
});

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

// Get last 20 tweets for a particular user
function getTweetsFromUsername(username) {
    var deferred = Q.defer();

    twitterClient.get('statuses/user_timeline', {
        screen_name: username,
        count: 10
    }, function(error, tweets, response) {
        if (!error) {
            deferred.resolve(tweets);
        } else {
            console.error(error);
            sentryClient.captureMessage(error);
            deferred.reject(new Error(error));
        }
    });

    return deferred.promise;
}

function formatToFeed(tweet, username) {
    return {
        'CreatedAt': tweet.CreatedAt,
        'Type': 'Tweet',

        // Headlines
        'Title': '',
        'Url': '',
        'Summary': '',
        'FeedURL': '',
        'PublicationId': 0,

        // Tweet specific
        'Username': username,
        'TweetId': tweet.TweetId,
        'TweetIdStr': tweet.TweetIdStr,
        'TwitterLikes': tweet.Likes,
        'TwitterRetweets': tweet.Retweets,

        // Instagram + Twitter
        'Text': tweet.Text
    };
}

// Add these tweets to ElasticSearch
// username here is the base parent username.
// Not just a username of any user.
function addToElastic(username, tweets) {
    var deferred = Q.defer();

    var user = tweets[0].user;
    var tweetsToAdd = [];

    for (var i = tweets.length - 1; i >= 0; i--) {
        var coordinates = '';
        if (tweets[i].coordinates && tweets[i].coordinates.coordinates && tweets[i].coordinates.coordinates.length === 2) {
            coordinates = tweets[i].coordinates.coordinates[0].toString() + ',' + tweets[i].coordinates.coordinates[1].toString();
        }

        var isRetweeted = false
        if (tweets[i].retweeted_status && tweets[i].retweeted_status.created_at) {
            isRetweeted = true;
        }

        tweetsToAdd.push({
            'TweetId': tweets[i].id,
            'TweetIdStr': tweets[i].id_str,
            'Text': tweets[i].text,
            'Likes': tweets[i].favorite_count,
            'Retweets': tweets[i].retweet_count,
            'Place': tweets[i].place && tweets[i].place.full_name || '',
            'Coordinates': coordinates,
            'Retweeted': isRetweeted,
            'CreatedAt': moment.unix(parseInt(tweets[i].timestamp_ms, 10) * 0.001).format('YYYY-MM-DDTHH:mm:ss') // damn you Twitter and your dates
        });
    }

    var esActions = [];
    for (var i = tweetsToAdd.length - 1; i >= 0; i--) {
        // Add to tweets endpoint
        var indexRecord = {
            index: {
                _index: 'tweets',
                _type: 'tweet',
                _id: tweetsToAdd[i].TweetId
            }
        };
        var dataRecord = tweetsToAdd[i];
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
                _id: tweetsToAdd[i].TweetId
            }
        };
        dataRecord = formatToFeed(tweetsToAdd[i], username);
        esActions.push(indexRecord);
        esActions.push({
            data: dataRecord
        });
    }

    // Add user to ElasticSearch as well
    var indexRecord = {
        index: {
            _index: 'tweets',
            _type: 'user',
            _id: username
        }
    };
    var dataRecord = user;
    dataRecord.Username = username;
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
        deferred.resolve(user);
    });

    return deferred.promise;
}

// Follow this user on Twitter to stream tweets
function followOnTwitter(user) {
    var deferred = Q.defer();

    twitterClient.post('friendships/create', {
        user_id: user.id,
        follow: true
    }, function(error, response) {
        if (error) {
            sentryClient.captureMessage(error);
            deferred.reject(error);   
        }
        deferred.resolve(true);
    });

    return deferred.promise;
}

// Process a particular Twitter user
function processTwitterUser(data) {
    var deferred = Q.defer();

    // Get tweets for a user
    getTweetsFromUsername(data.username).then(function(tweets) {
        // Add tweets to elasticsearch
        addToElastic(data.username, tweets).then(function(user) {
            if (user) {
                // Follow the user on the NewsAIHQ Twitter so we can stream the
                // Tweets later.
                followOnTwitter(user).then(function(response) {
                    deferred.resolve(true);
                }, function(error) {
                    sentryClient.captureMessage(error);
                    deferred.reject(error);
                });
            } else {
                var error = new Error('Elasticsearch add failed');
                sentryClient.captureMessage(error);
                deferred.reject(error);
            }
        }, function(error) {
            sentryClient.captureMessage(error);
            deferred.reject(error);
        });

    }, function(error) {
        sentryClient.captureMessage(error);
        deferred.reject(error);
    });

    return deferred.promise;
}

// Subscribe to Pub/Sub for this particular topic
function subscribe(cb) {
    var subscription;

    // Event handlers
    function handleMessage(message) {
        cb(null, message);
    }

    function handleError(err) {
        sentryClient.captureMessage(err);
        console.error(err);
    }

    getTopic(function(err, topic) {
        if (err) {
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

// Begin subscription
subscribe(function(err, message) {
    // Any errors received are considered fatal.
    if (err) {
        sentryClient.captureMessage(err);
        console.error(err);
        throw err;
    }
    console.log('Received request to process twitter feed ' + message.data.username);
    processTwitterUser(message.data)
        .then(function(status) {
            rp('https://hchk.io/1a7203c6-1716-4933-bdc0-673c4cd2d7bd')
                .then(function (htmlString) {
                    console.log('Completed execution for ' + message.data.username);
                })
                .catch(function (err) {
                    console.error(err);
                });
        }, function(error) {
            sentryClient.captureMessage(error);
            console.error(error);
        });
});

// // Code for testing the functions above
// var message = {
//     data: {
//         username: 'abhiagarwal'
//     }
// };

// processTwitterUser(message.data)
//     .then(function(status) {
//         console.log('Completed execution for ' + message.data.username);
//     }, function(error) {
//         console.error(error);
//     });