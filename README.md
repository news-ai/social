# social

Getting data from RSS, and twitter through Google Cloud Functions, Pub/Sub, and Google Compute Engine.

Feed loops through all the feeds in the API that haven't been processed. It calls the RSS function for each feed. The RSS function gets the RSS headlines and writes them to ES.

### RSS

```
function testProcess() {
    var data = {};
    data.contactId = 4934182044172288;
    data.url = 'http://pagesix.com/author/cindy-adams/feed/';
    return getContent(data);
};
```

Pub/Sub call:

```json
{"url":"http://pagesix.com/author/cindy-adams/feed/", "contactId": 4934182044172288}
```
