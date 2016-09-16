var feedparser = require('feedparser-promised');
var Q = require('q');

function getFeedFromUrl(url) {
    var deferred = Q.defer();

    feedparser.parse(url).then(function(items) {
        var contents = [];
        items.forEach(function(item) {
            var content = {
                title: item.title,
                author: item.author
            }
            contents.push(content);
        });
        deferred.resolve(contents);
    }).catch(function(error) {
        deferred.reject(new Error(error));
        throw new Error(error);
    });

    return deferred.promise;
}

getFeedFromUrl('http://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml')
    .then(function(data) {
        console.log(data);
    }, function(error) {
        console.log(error);
    });