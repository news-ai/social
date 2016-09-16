# social

Getting data from RSS, and twitter through Google Cloud Functions.

Feed loops through all the feeds in the API that haven't been processed. It calls the RSS function for each feed. The RSS function gets the RSS headlines and writes them to ES.

### RSS

Deploy RSS: `cd rss` then `gcloud alpha functions deploy processRSS --bucket datastore_elastic_api_sync --trigger-http --region us-central1`

Get logs for RSS: `gcloud alpha functions get-logs processRSS`
