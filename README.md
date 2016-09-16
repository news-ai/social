# social

Getting data from RSS, and twitter through Google Cloud Functions.

Feed loops through all the feeds in the API that haven't been processed. It calls the RSS function for each feed. The RSS function gets the RSS headlines and writes them to ES.
