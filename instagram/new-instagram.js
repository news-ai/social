var request = require('request');

var access_token = '43004312.4314d27.3e8c7280a4ec49119e240d8cbaaa89c4';
var username = 'abhiagarwal';

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

        console.log(instagramData);
    }
})