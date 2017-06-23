'use strict';

var elasticsearch = require('elasticsearch');

// Instantiate a elasticsearch client
var elasticSearchClient = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search1.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

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

        // Tweet
        'TweetId': tweet.TweetId,
        'TweetIdStr': tweet.TweetIdStr,
        'TwitterLikes': tweet.Likes,
        'TwitterRetweets': tweet.Retweets,
        'Text': tweet.Text,
        'Username': username,
    };
}

function addTweetToEs(tweet, username) {
    var deferred = Q.defer();

    var esActions = [];

    var coordinates = '';
    if (tweet.coordinates && tweet.coordinates.coordinates && tweet.coordinates.coordinates.length === 2) {
        coordinates = tweet.coordinates.coordinates[0].toString() + ',' + tweet.coordinates.coordinates[1].toString();
    }

    var isRetweeted = false
    if (tweet.retweeted_status && tweet.retweeted_status.created_at) {
        isRetweeted = true;
    }

    var tweetToAdd = {
        'TweetId': tweet.id,
        'TweetIdStr': tweet.id_str,
        'Text': tweet.text,
        'Likes': tweet.favorite_count,
        'Retweets': tweet.retweet_count,
        'Place': tweet.place && tweet.place.full_name || '',
        'Coordinates': coordinates,
        'Retweeted': isRetweeted,
        'CreatedAt': moment(tweet.created_at).format('YYYY-MM-DDTHH:mm:ss')
    };

    var indexRecord = {
        index: {
            _index: 'tweets',
            _type: 'tweet',
            _id: tweet.id
        }
    };

    var dataRecord = tweetToAdd;
    dataRecord.Username = username;
    esActions.push(indexRecord);
    esActions.push({
        data: dataRecord
    });

    indexRecord = {
        index: {
            _index: 'feeds',
            _type: 'feed',
            _id: tweet.id
        }
    };
    dataRecord = formatToFeed(tweetToAdd, username);
    esActions.push(indexRecord);
    esActions.push({
        data: dataRecord
    });

    elasticSearchClient.bulk({
        body: esActions
    }, function(error, response) {
        if (error) {
            console.error(error);
            sentryClient.captureMessage(error);
            deferred.reject(error);
        }
        deferred.resolve(true);
    });

    return deferred.promise;
}

function findUsernameFromTwitterId(twitterId) {
    var deferred = Q.defer();

    elasticSearchClient.search({
        q: 'data.id:' + twitterId
    }).then(function(body) {
        var hits = body.hits.hits;
        if (hits.length > 0) {
            deferred.resolve(hits[0]._source.data.screen_name);
        } else {
            var error = 'Did not get any hits';
            console.error(error);
            sentryClient.captureMessage(error);
            deferred.reject(error);
        }
    }, function(error) {
        console.error(error);
        sentryClient.captureMessage(error);
        deferred.reject(error);
    });

    return deferred.promise;
}