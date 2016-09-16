var feedparser = require('feedparser-promised');
var elasticsearch = require('elasticsearch');
var Q = require('q');
var mocks = require('node-mocks-http');

// Instantiate a elasticsearch client
var client = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

function getFeedFromUrl(url) {
    var deferred = Q.defer();

    feedparser.parse(url).then(function(items) {
        var contents = [];
        items.forEach(function(item) {
            var content = {
                Title: item.title,
                Author: item.author,
                Url: item.guid,
                Categories: item.categories,
                PublishDate: item.pubDate,
                Summary: item.summary
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

    var esActions = [];
    for (var i = content.length - 1; i >= 0; i--) {
        var indexRecord = {
            index: {
                _index: 'headlines',
                _type: 'headline'
            }
        };
        var dataRecord = content[i];
        dataRecord.ContactId = contactId;
        esActions.push(indexRecord);
        esActions.push({
            data: dataRecord
        });
    }

    client.bulk({
        body: esActions
    }, function(error, response) {
        if (error) {
            console.error(error);
            deferred.resolve(false);
        }
        deferred.resolve(true);
    });

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
    req.body.url = 'http://nypost.com/author/michael-goodwin/feed/';
    return getContent(req, res);
};

testProcess();
