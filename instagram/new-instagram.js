var request = require('request');

var access_token = '43004312.4314d27.3e8c7280a4ec49119e240d8cbaaa89c4';

request('https://api.instagram.com/v1/users/larosequotidien/media/recent/?access_token=' + access_token, function (error, response, body) {
    console.log(error);
  if (!error && response.statusCode == 200) {
    console.log(body) // Show the HTML for the Google homepage.
  }
})