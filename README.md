# social

Getting data from RSS, and twitter through Google Cloud Functions, Pub/Sub, and Google Compute Engine.

Feed loops through all the feeds in the API that haven't been processed. It calls the RSS function for each feed. The RSS function gets the RSS headlines and writes them to ES.

### RSS

Deployed on Google Compute Engine. Just need to send information to the Pub/Sub topic: `process-rss-feed`.

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
{
    "url": "http://pagesix.com/author/cindy-adams/feed/",
    "contactId": 4934182044172288
}
```

### Feeds

Deployed on Google Compute Engine. Will run periodically to check any feed that needs to get fetched. The input is data from the datastore, and the output is a pub/sub call to `process-rss-feed` with `url` and `contactId`.

### Compute Engine

`gcloud compute --project "newsai-1166" ssh --zone "us-east1-c" "feeds-3"`

### Twitter

- `new-twitter.js` takes Pub/Sub of `username, contactId`.
- `stream-twitter.js` doesn't need anything to run.
