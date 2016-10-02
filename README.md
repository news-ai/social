# social

Getting data from RSS, and twitter through Google Cloud Functions, Pub/Sub, and Google Compute Engine.

Feed loops through all the feeds in the API that haven't been processed. It calls the RSS function for each feed. The RSS function gets the RSS headlines and writes them to ES.

### RSS

Deployed on Google Compute Engine. Just need to send information to the Pub/Sub topic: `process-rss-feed`.

```
function testProcess() {
    var data = {};
    data.publicationId = 6539064085839872;
    data.url = 'http://pagesix.com/author/cindy-adams/feed/';
    return getContent(data);
};
```

Pub/Sub call:

```json
{
    "url": "http://pagesix.com/author/cindy-adams/feed/",
    "publicationId": 6539064085839872
}
```

### Feeds

Deployed on Google Compute Engine. Will run periodically to check any feed that needs to get fetched. The input is data from the datastore, and the output is a pub/sub call to `process-rss-feed` with `username`.

### Twitter

- `new-twitter.js` takes Pub/Sub of `username`.

Pub/Sub call:

```json
{
    "username": "abhiagarwal"
}
```

- `stream-twitter.js` doesn't need anything to run.

### Instagram

Pub/Sub call:

- `new-instagram.js` takes Pub/Sub of `username`, and `access_token`.

```json
{
    "access_token": "43004312.4314d27.3e8c7280a4ec49119e240d8cbaaa89c4",
    "username": "abhiagarwal"
}
```

### Compute Engine

`gcloud compute --project "newsai-1166" ssh --zone "us-east1-c" "feeds-3"`
