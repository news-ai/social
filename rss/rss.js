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
                author: item.author,
                url: item.guid,
                categories: item.categories,
                publishDate: item.pubDate,
                summary: item.summary
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

function addToElastic(contactId, content) {
    var deferred = Q.defer();

    console.log(contactId);

    return deferred.promise;
}

function getContent(req, res) {
    var deferred = Q.defer();

    getFeedFromUrl(req.body.url)
        .then(function(content) {
            addToElastic(req.body.contactId, content)
                .then(function(status) {
                    if (status) {
                        deferred.resolve(true);
                        res.status(200).end();
                    } else {
                        var error = 'Elasticsearch add failed';
                        console.error(error);
                        deferred.resolve(false);
                        res.status(500).send(error);
                    }
                }, function (error) {
                    console.error(error);
                    deferred.resolve(false);
                    res.status(500).send();
                });
        }, function(error) {
            console.error(error);
            deferred.reject(new Error(error));
            res.status(500).send(error);
        });

    return deferred.promise;
}

exports.processRSS = function processRSS(req, res) {
    return getContent(req, res);
};

function testProcess() {
    req = mocks.createRequest();
    res = mocks.createResponse();
    req.body.contactId = 5702224873259008;
    req.body.url = 'http://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml';
    return getContent(req, res);
};

testProcess();
