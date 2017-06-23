# social

[Dashboard](https://app.keymetrics.io/#/bucket/57fe822672900b983297f577/dashboard)

Getting data from RSS, Instagram, and Twitter through Pub/Sub, and Google Compute Engine.

Feed loops through all the feeds in the API that haven't been processed. It calls the RSS function for each feed. The RSS function gets the RSS headlines and writes them to ES.

### Deployment notes

`pm2 start app.js -i max`

Services to start (in this order):

- `tabulae/rss/rss.js`
- `tabulae/feed/feed.js`
- `tabulae/instagram/new-instagram.js`
- `tabulae/instagram/stream-instagram.js`
- `tabulae/instagram/sync-profiles.js`
- `tabulae/instagram/sync-posts.js`
- `tabulae/twitter/new-twitter.js`
- `twitter/twitter/stream-twitter.js`
- `twitter/twitter/sync-profiles.js`
- `twitter/twitter-sync-posts.js`
- `md/twitter/new-twitter.js`
- `md/twitter/twitter-sync-posts.js`

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

Deployed on Google Compute Engine. Will run periodically to check any feed that needs to get fetched. The input is data from the datastore, and the output is a pub/sub call to `process-rss-feed` with `username`. 2 functions:

1. Get initial data for Twitter user and 20 posts (new)
2. Get new tweets as they come in (stream)

### Twitter

- `new-twitter.js` takes Pub/Sub of `username`.

Pub/Sub call:

```json
{
    "username": "abhiagarwal"
}
```

- `stream-twitter.js` doesn't need anything to run.
- `sync-profiles.js` doesn't need anything to run.
- `sync-posts.js` doesn't need anything to run.

### Instagram

3 functions:

1. Get initial data for Instagram user and posts (new)
2. Repeatedly keep the data up to date (sync (posts & profiles))
3. Get new posts as they come out (stream)

Pub/Sub call:

- `new-instagram.js` takes Pub/Sub of `username`. Sometimes it passes in `depth`.

```json
{
    "username": "abhiagarwal"
}
```

- `sync-posts.js` does not take any arguments. It reads from `/instagrams/instagram/_search` on ES for the last week.
- `sync-profiles.js` does not take any arguments. It reads from `/instagrams/user/_search` on ES.
- `stream-instagram.js`

### Timeseries

Creating a timeseries of data for both Twitter and Instagram data.

- `twitter.js`
- `instagram.js`

### Compute Engine

`gcloud compute --project "newsai-1166" ssh --zone "us-east1-c" "social-1"`

### Microsoft Azure

`ssh api@104.45.156.57`
