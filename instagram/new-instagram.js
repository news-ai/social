/*!
 * Gets new posts for a particular Instagram user
 * With or without an access token
 */

'use strict';

var Q = require('q');
var rp = require('request-promise');
var request = require('requestretry');
var elasticsearch = require('elasticsearch');
var moment = require('moment');
var raven = require('raven');

var instagram = require('./instagram');

// Instantiate a elasticsearch client
var elasticSearchClient = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search1.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

// Initialize Google Cloud
var topicName = 'process-instagram-feed';
var subscriptionName = 'node-new-user-instagram';

// Instantiate a sentry client
var sentryClient = new raven.Client('https://4db5dd699d4a4267ab6f56fa97a9ee5c:9240e5b57b864de58f514b6d40e7e5a7@sentry.io/103131');
sentryClient.patchGlobal();

function addFeedToPubSub(instagramUser) {
    var deferred = Q.defer();

    instagram.getTopic(topicName, function(err, topic) {
        if (err) {
            deferred.reject(new Error(err));
            console.error('Error occurred while getting pubsub topic', err);
            sentryClient.captureMessage(err);
        } else {
            topic.publish({
                data: {
                    username: instagramUser,
                    access_token: '',
                    depth: 1
                }
            }, function(err) {
                if (err) {
                    deferred.reject(new Error(err));
                    console.error('Error occurred while queuing background task', err);
                    sentryClient.captureMessage(err);
                } else {
                    deferred.resolve(true);
                    console.info('Instagram user ' + instagramUser + ' sent to ' + topicName + ' pubsub');
                }
            });
        }
    });

    return deferred.promise;
}

// Add these instagram posts to ElasticSearch
// username here is the base parent username.
// Not just a username of any user.
function addToElastic(username, posts, profile, isFormatted) {
    var deferred = Q.defer();

    var esActions = [];
    // Look through all the instagram data
    for (var i = posts.data.length - 1; i >= 0; i--) {
        var newInstagramPost = {};
        var newInstagramPostId = posts.data[i].id;

        if (!isFormatted) {
            newInstagramPost.CreatedAt = moment.unix(parseInt(posts.data[i].created_time, 10)).format('YYYY-MM-DDTHH:mm:ss');
            newInstagramPost.Video = posts.data[i].videos && posts.data[i].videos.standard_resolution && posts.data[i].videos.standard_resolution.url || '';
            newInstagramPost.Image = posts.data[i].images && posts.data[i].images.standard_resolution && posts.data[i].images.standard_resolution.url || '';
            newInstagramPost.Location = posts.data[i].location && posts.data[i].location.name || '';

            var coordinates = '';
            if (posts.data[i].location && posts.data[i].location.latitude && posts.data[i].location.longitude) {
                coordinates = posts.data[i].location.latitude.toString() + ',' + posts.data[i].location.longitude.toString();
            }

            newInstagramPost.Coordinates = coordinates;
            newInstagramPost.InstagramId = posts.data[i].id || '';
            newInstagramPost.Caption = posts.data[i].caption && posts.data[i].caption.text || '';
            newInstagramPost.Likes = posts.data[i].likes && posts.data[i].likes.count || 0;
            newInstagramPost.Comments = posts.data[i].comments && posts.data[i].comments.count || 0;
            newInstagramPost.Link = posts.data[i].link || '';

            newInstagramPost.InstagramWidth = posts.data[i].images && posts.data[i].images.standard_resolution && posts.data[i].images.standard_resolution.width || 0;
            newInstagramPost.InstagramHeight = posts.data[i].images && posts.data[i].images.standard_resolution && posts.data[i].images.standard_resolution.height || 0;

            var tags = [];
            if (posts.data[i].tags && posts.data[i].tags.length > 0) {
                tags = posts.data[i].tags;
            }

            newInstagramPost.Tags = tags;
        } else {
            newInstagramPost = posts.data[i];
            delete newInstagramPost.id;
        }

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

    if (profile && profile.data) {
        // Add user to ElasticSearch as well
        var indexRecord = {
            index: {
                _index: 'instagrams',
                _type: 'user',
                _id: username
            }
        };
        var dataRecord = profile.data;
        dataRecord.Username = username;
        esActions.push(indexRecord);
        esActions.push({
            data: dataRecord
        });
    }

    if (esActions.length > 0) {
        elasticSearchClient.bulk({
            body: esActions
        }, function(error, response) {
            if (error) {
                sentryClient.captureMessage(error);
                deferred.reject(error);
            }
            deferred.resolve(true);
        });
    } else {
        var error = 'Nothing was added to ES for username ' + username;
        sentryClient.captureMessage(error);
        deferred.reject(error);
    }

    return deferred.promise;
}

function PageNotFound(err, response, body){
  // retry the request if we had an error or if the response was a 'Page Not Found'
  return err || response.statusCode === 404;
}

function getInstagramIdFromUsername(username) {
    var deferred = Q.defer();

    request({
        url: 'https://www.instagram.com/' + username + '/?__a=1',
        maxAttempts: 5,
        retryDelay: 3000,
        retryStrategy: PageNotFound
    }, function(error, response, body) {
        if (!error && response.statusCode == 200) {
            var userProfile = JSON.parse(body);
            if (userProfile.user && userProfile.user.id) {
                deferred.resolve(userProfile.user.id);
            } else {
                var error = 'Could not find Instagram Id';
                console.error(error);
                sentryClient.captureMessage(error);
                deferred.reject(new Error(error));
            }
        } else {
            console.error(error);
            sentryClient.captureMessage(error);
            deferred.reject(new Error(error));
        }
    });

    return deferred.promise;
}

function getInstagramProfileFromUsernameWithAccessToken(data, userid) {
    var deferred = Q.defer();

    request('https://api.instagram.com/v1/users/' + userid + '/?access_token=' + data.access_token, function(error, response, body) {
        if (!error && response.statusCode == 200) {
            var instagramProfile = JSON.parse(body);
            deferred.resolve(instagramProfile);
        } else {
            console.error(body);
            sentryClient.captureMessage(body);
            deferred.reject(new Error(body));
        }
    })

    return deferred.promise;
}

function getInstagramFromUsernameWithAccessToken(data) {
    var deferred = Q.defer();

    getInstagramIdFromUsername(data.username).then(function(userid) {
        request('https://api.instagram.com/v1/users/' + userid + '/media/recent/?access_token=' + data.access_token, function(error, response, body) {
            if (!error && response.statusCode == 200) {
                var instagramMedia = JSON.parse(body);
                deferred.resolve([userid, instagramMedia]);
            } else {
                console.error(body);
                sentryClient.captureMessage(body);
                deferred.reject(new Error(body));
            }
        })
    }, function(error) {
        console.error(error);
        sentryClient.captureMessage(error);
        deferred.reject(new Error(error));
    });

    return deferred.promise;
}

function getInstagramFromPostId(postId) {
    var deferred = Q.defer();

    request('https://www.instagram.com/p/' + postId + '/?__a=1', function(error, response, body) {
        if (!error && response.statusCode == 200) {
            var instagramPost = JSON.parse(body);
            var instagramMedia = instagramPost.media;
            deferred.resolve(instagramMedia);
        } else {
            console.error(body);
            sentryClient.captureMessage(body);
            deferred.reject(new Error(body));
        }
    });

    return deferred.promise;
}

function getInstagramFromNodes(media) {
    var allPromises = [];
    for (var i = media.nodes.length - 1; i >= 0; i--) {
        var toExecute = getInstagramFromPostId(media.nodes[i].code);
        allPromises.push(toExecute);
    }
    return Q.all(allPromises);
}

function getInstagramFromUsernameWithoutAccessToken(data) {
    var deferred = Q.defer();

    request({
        url: 'https://www.instagram.com/' + data.username + '/?__a=1',
        maxAttempts: 3,
        retryDelay: 3000,
        retryStrategy: PageNotFound
    }, function(error, response, body) {
        if (!error && response.statusCode == 200) {
            var instagramMedia = JSON.parse(body);
            var instagramUser = instagramMedia.user;

            // If the set user is private
            if (instagramUser && instagramUser.is_private) {
                var apiData = {
                    'network': 'Instagram',
                    'username': data.username,
                    'privateorinvalid': 'Private'
                };

                // Set the user to be private
                request({
                    url: 'https://tabulae.newsai.org/tasks/socialUsernameInvalid',
                    method: 'POST',
                    json: apiData,
                    auth: {
                        user: 'jebqsdFMddjuwZpgFrRo',
                        password: ''
                      }
                }, function(error, response, body) {
                    if (!error && response.statusCode == 200) {
                        console.log('User sent to be private');
                        deferred.resolve([instagramUser, []]);
                    } else {
                        console.error(error);
                        console.error(body);
                        sentryClient.captureMessage(body);
                        deferred.reject(new Error(body));
                    }
                });
            } else {
                // If it not private
                if (instagramUser && instagramUser.media && instagramUser.media.count > 0) {
                    // Get all their content
                    getInstagramFromNodes(instagramUser.media).then(function(responses) {
                        deferred.resolve([instagramUser, responses]);
                    }, function(error) {
                        console.error(error);
                        sentryClient.captureMessage(error);
                        deferred.reject(new Error(error));
                    });
                } else {
                    // If the count is empty
                    deferred.resolve([instagramUser, []]);
                }
            }
        } else {
            if (!('depth' in data)) {
                addFeedToPubSub(data.username).then(function (status) {
                    if (status) {
                        var error = 'Error occured, but sent another pubsub';
                        sentryClient.captureMessage(error);
                        deferred.reject(new Error(error));
                    } else {
                        var error = 'Error occured, but could not send another pubsub';
                        sentryClient.captureMessage(error);
                        deferred.reject(new Error(error));
                    }
                }, function (error) {
                    sentryClient.captureMessage(error);
                    deferred.reject(new Error(error));
                });
            } else {
                // Invalidate the Instagram User here before sending the error
                var apiData = {
                    'network': 'Instagram',
                    'username': data.username,
                    'privateorinvalid': 'Invalid'
                };

                // Set the user to be invalid
                request({
                    url: 'https://tabulae.newsai.org/tasks/socialUsernameInvalid',
                    method: 'POST',
                    json: apiData,
                    auth: {
                        user: 'jebqsdFMddjuwZpgFrRo',
                        password: ''
                      }
                }, function(error, response, body) {
                    if (!error && response.statusCode == 200) {
                        console.log('User sent to be invalid');
                        deferred.resolve([instagramUser, []]);
                    } else {
                        console.error(error);
                        console.error(response.statusCode);
                        console.error(body);
                        sentryClient.captureMessage(body);
                        deferred.reject(new Error(body));
                    }
                });
            }
        }
    });

    return deferred.promise;
}

function formatInstagramUserAndPosts(instagramUserAndPosts) {
    var instagramUser = instagramUserAndPosts[0];
    var instagramPosts = instagramUserAndPosts[1];

    var user = {
        'data': {
            'username': instagramUser.username || '',
            'bio': instagramUser.biography || '',
            'website': instagramUser.external_url || '',
            'profile_picture': instagramUser.profile_pic_url || '',
            'full_name': instagramUser.full_name || '',
            'counts': {
                'media': instagramUser.media.count || 0,
                'followed_by': instagramUser.followed_by && instagramUser.followed_by.count || 0,
                'follows': instagramUser.follows && instagramUser.follows.count || 0
            },
            'id': instagramUser.id || ''
        }
    };

    var posts = [];
    for (var i = instagramPosts.length - 1; i >= 0; i--) {
        var instagramId = [instagramPosts[i].id, instagramPosts[i].owner.id].join('_');
        var tags = instagramPosts[i].caption && instagramPosts[i].caption.match(/#[a-z]+/gi) || [];

        var post = {
            'CreatedAt': moment.unix(parseInt(instagramPosts[i].date, 10)).format('YYYY-MM-DDTHH:mm:ss'),
            'Video': instagramPosts[i].video_url || '',
            'Image': instagramPosts[i].display_src || '',
            'Location': instagramPosts[i].location && instagramPosts[i].location.name || '',
            'Coordinates': '',
            'InstagramId': instagramId || '',
            'Caption': instagramPosts[i].caption || '',
            'Likes': instagramPosts[i].likes && instagramPosts[i].likes.count || 0,
            'Comments': instagramPosts[i].comments && instagramPosts[i].comments.count || 0,
            'Link': 'https://www.instagram.com/p/' + instagramPosts[i].code + '/' || '',
            'Tags': tags || [],
            'id': instagramId || '',
            'InstagramHeight': instagramPosts[i].dimensions && instagramPosts[i].dimensions.height || 0,
            'InstagramWidth': instagramPosts[i].dimensions && instagramPosts[i].dimensions.width || 0,
        };
        posts.push(post);
    }

    posts = {
        'data': posts
    };

    return [user, posts];
}

// Process a particular Instagram user
function processInstagramUser(data) {
    var deferred = Q.defer();

    if (data.access_token !== '') {
        // Get instagram post for a user
        getInstagramFromUsernameWithAccessToken(data).then(function(instagramIdAndPosts) {
            // Get instagram profile for a user
            getInstagramProfileFromUsernameWithAccessToken(data, instagramIdAndPosts[0]).then(function(profile) {
                // Add instagram posts to elasticsearch
                addToElastic(data.username, instagramIdAndPosts[1], profile, false).then(function(status) {
                    if (status) {
                        deferred.resolve(status);
                    } else {
                        var error = 'Could not add instagram posts to ES'
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
        }, function(error) {
            sentryClient.captureMessage(error);
            deferred.reject(error);
        });
    } else {
        // If there is no access_token passed into the process
        getInstagramFromUsernameWithoutAccessToken(data).then(function(instagramUserAndPosts) {
            instagramUserAndPosts = formatInstagramUserAndPosts(instagramUserAndPosts);
            // Add instagram posts to elasticsearch
            addToElastic(data.username, instagramUserAndPosts[1], instagramUserAndPosts[0], true).then(function(status) {
                if (status) {
                    deferred.resolve(status);
                } else {
                    var error = 'Could not add instagram posts to ES'
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
    }

    return deferred.promise;
}

// Begin subscription
instagram.subscribe(topicName, subscriptionName, function(err, message) {
    // Any errors received are considered fatal.
    if (err) {
        console.error(err);
        sentryClient.captureMessage(err);
        throw err;
    }
    console.log('Received request to process instagram feed ' + message.data.username);
    processInstagramUser(message.data)
        .then(function(status) {
            rp('https://hchk.io/27266425-6884-400a-8c54-4a9f3e2c4026')
                .then(function (htmlString) {
                    console.log('Completed execution for ' + message.data.username);
                })
                .catch(function (err) {
                    console.error(err);
                });
        }, function(error) {
            console.error(error);
            sentryClient.captureMessage(error);
        });
});

// // Code for testing the functions above
// var message = {
//     data: {
//         access_token: '',
//         username: 'coolbeancool'
//     }
// };

// processInstagramUser(message.data)
//     .then(function(status) {
//         console.log('Completed execution for ' + message.data.username);
//     }, function(error) {
//         console.error(error);
//     });