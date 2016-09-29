var Q = require('q');
var request = require('request');

function getInstagramFromUsername(access_token, username) {
    var deferred = Q.defer();

    request('https://api.instagram.com/v1/users/self/media/recent/?access_token=' + access_token, function(error, response, body) {
        if (!error && response.statusCode == 200) {
            var instagramMedia = JSON.parse(body);
            var instagramUser = instagramMedia.data[0].user;
            var instagramData = [];

            // Look through all the instagram data
            for (var i = instagramMedia.data.length - 1; i >= 0; i--) {
                delete instagramMedia.data[i].user;
                instagramMedia.data[i].Username = username;
                instagramData.push(instagramMedia.data[i]);
            }

            deferred.resolve(instagramData);
        } else {
            console.error(error);
            deferred.reject(new Error(error));
        }
    })

    return deferred.promise;
}

// Process a particular Twitter user
function processInstagramUser(data) {
    var deferred = Q.defer();

    // Get tweets for a user
    getInstagramFromUsername(data.access_token, data.username).then(function(posts) {
        // Add instagram posts to elasticsearch
        console.log(posts);

    }, function(error) {
        deferred.reject(error);
    });

    return deferred.promise;
}

// Code for testing the functions above
var message = {
    data: {
        access_token: '43004312.4314d27.3e8c7280a4ec49119e240d8cbaaa89c4',
        username: 'abhiagarwal'
    }
};

processInstagramUser(message.data)
    .then(function(status) {
        console.log('Completed execution for ' + message.data.username);
    }, function(error) {
        console.error(error);
    });