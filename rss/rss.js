var feedparser = require('feedparser-promised');
var Q = require('q');
var mocks = require('node-mocks-http');

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

function getContents(req, res) {
    var deferred = Q.defer();

    getFeedFromUrl(req.body.url)
        .then(function(data) {
            console.log(data);
            res.status(200).end();
        }, function(error) {
            console.error(error);
            deferred.reject(new Error(error));
            res.status(500).send(error);
        });

    return deferred.promise;
}

exports.processFeed = function processFeed(req, res) {
    return getContents();
};

function testProcess() {
    req = mocks.createRequest();
    res = mocks.createResponse();
    req.body.url = 'http://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml';
    return getContents(req, res);
};

testProcess();
