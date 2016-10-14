'use strict';

var Q = require('q');
var request = require('requestretry');
var elasticsearch = require('elasticsearch');

// Instantiate a elasticsearch client
var elasticSearchClient = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search1.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

var instagram = exports;

instagram.formatToFeed = function (post, username) {
    return {
        'CreatedAt': post.CreatedAt,
        'Type': 'Instagram',

        // Headlines
        'Title': '',
        'Url': '',
        'Summary': '',
        'FeedURL': '',
        'PublicationId': 0,

        // Tweet
        'TweetId': 0,
        'Username': '',

        // Tweet + Instagram
        'Text': post.Caption,

        // Instagram
        'InstagramUsername': username,
        'InstagramId': post.InstagramId,
        'InstagramImage': post.Image,
        'InstagramVideo': post.Video,
        'InstagramLink': post.Link,
        'InstagramLikes': post.Likes,
        'InstagramComments': post.Comments,
        'InstagramWidth': post.InstagramWidth,
        'InstagramHeight': post.InstagramHeight,
    };
}

function getInstagramPageFromEs(offset) {
    var deferred = Q.defer();

    elasticSearchClient.search({
        index: 'instagrams',
        type: 'user',
        body: {
            "query": {
                "filtered": {
                    "query": {
                        "match_all": {}
                    }
                }
            },
            "size": 100,
            "from": 100 * offset
        }
    }).then(function(resp) {
        var hits = resp.hits.hits;
        deferred.resolve(hits);
    }, function(err) {
        console.trace(err.message);
    });

    return deferred.promise;
}

function getInstagramProfiles(offset, allData) {
    var deferred = Q.defer();

    getInstagramPageFromEs(offset).then(function(data) {
        if (data.length === 0) {
            deferred.resolve(allData);
        } else {
            var newData = allData.concat(data);
            deferred.resolve(getInstagramProfiles(offset + 1, newData));
        }
    });

    return deferred.promise;
}

function getInstagramProfilesFromAPI(data) {
    var allPromises = [];
    for (var i = data.length - 1; i >= 0; i--) {
        var url = 'https://www.instagram.com/' + data[i]._id + '/';
        var toExecute = getInstagramFromProfileLink(url);
        allPromises.push(toExecute);
    }
    return Q.allSettled(allPromises);
}

function getInstagramFromProfileLink(profileLink) {
    var deferred = Q.defer();

    request(profileLink + '?__a=1', function(error, response, body) {
        if (!error && response.statusCode == 200) {
            var instagramProfile = JSON.parse(body);
            var instagramUser = instagramProfile.user;
            deferred.resolve(instagramUser);
        } else {
            sentryClient.captureMessage(body);
            deferred.reject(new Error(body));
        }
    });

    return deferred.promise;
}

function getInstagramFromPostLink(postLink) {
    var deferred = Q.defer();

    request(postLink + '?__a=1', function(error, response, body) {
        if (!error && response.statusCode == 200) {
            var instagramPost = JSON.parse(body);
            var instagramMedia = instagramPost.media;
            deferred.resolve(instagramMedia);
        } else {
            sentryClient.captureMessage(body);
            deferred.reject(new Error(body));
        }
    });

    return deferred.promise;
}

function getInstagramPostsFromAPI(data) {
    var allPromises = [];
    for (var i = data.length - 1; i >= 0; i--) {
        var toExecute = getInstagramFromPostLink(data[i]._source.data.Link);
        allPromises.push(toExecute);
    }
    return Q.allSettled(allPromises);
}

function getInstagramPageFromEsLastWeek(offset) {
    var deferred = Q.defer();

    var dateTo = moment().format('YYYY-MM-DD');
    var dateFrom = moment().subtract(7, 'd')
    var lastWeek = dateFrom.format('YYYY-MM-DDTHH:mm:ss');

    elasticSearchClient.search({
        index: 'instagrams',
        type: 'instagram',
        body: {
            "query": {
                "filtered": {
                    "query": {
                        "match_all": {}
                    },
                    "filter": {
                        "range": {
                            "data.CreatedAt": {
                                "gte": lastWeek
                            }
                        }
                    }
                }
            },
            "sort": [{
                "data.CreatedAt": {
                    "order": "desc",
                    "mode": "avg"
                }
            }],
            "size": 100,
            "from": 100 * offset,
        }
    }).then(function(resp) {
        var hits = resp.hits.hits;
        deferred.resolve(hits);
    }, function(err) {
        console.trace(err.message);
    });

    return deferred.promise;
}

function getInstagramPostsFromEsLastWeek(offset, allData) {
    var deferred = Q.defer();

    getInstagramPageFromEsLastWeek(offset).then(function(data) {
        if (data.length === 0) {
            deferred.resolve(allData);
        } else {
            var newData = allData.concat(data);
            deferred.resolve(getInstagramPostsFromEsLastWeek(offset + 1, newData));
        }
    });

    return deferred.promise;
}

instagram.getInstagramProfiles = getInstagramProfiles;
instagram.getInstagramProfilesFromAPI = getInstagramProfilesFromAPI;
instagram.getInstagramPostsFromAPI = getInstagramPostsFromAPI;
instagram.getInstagramPostsFromEsLastWeek = getInstagramPostsFromEsLastWeek;