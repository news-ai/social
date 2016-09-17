'use strict';

var Q = require('q');

// Instantiate a datastore client
var datastore = require('@google-cloud/datastore')({
    projectId: 'newsai-1166'
});

/**
 * Gets a Datastore key from the kind/key pair in the request.
 *
 * @param {Object} requestData Cloud Function request data.
 * @param {string} requestData.Id Datastore ID string.
 * @returns {Object} Datastore key object.
 */
function getKeyFromRequestData(requestData, resouceType) {
    if (!requestData.Id) {
        throw new Error('Id not provided. Make sure you have a "Id" property ' +
            'in your request');
    }

    var listId = parseInt(requestData.Id, 10);
    return datastore.key([resouceType, listId]);
}

/**
 * Retrieves a record.
 *
 * @example
 * gcloud alpha functions call ds-get --data '{"kind":"gcf-test","key":"foobar"}'
 *
 * @param {Object} context Cloud Function context.
 * @param {Function} context.success Success callback.
 * @param {Function} context.failure Failure callback.
 * @param {Object} data Request data, in this case an object provided by the user.
 * @param {string} data.kind The Datastore kind of the data to retrieve, e.g. "user".
 * @param {string} data.key Key at which to retrieve the data, e.g. 5075192766267392.
 */
function getDatastore(data, resouceType) {
    var deferred = Q.defer();
    try {
        var key = getKeyFromRequestData(data, resouceType);

        datastore.get(key, function(err, entity) {
            if (err) {
                console.error(err);
                deferred.reject(new Error(error));
            }

            // The get operation will not fail for a non-existent entity, it just
            // returns null.
            if (!entity) {
                var error = 'Entity does not exist';
                console.error(error);
                deferred.reject(new Error(error));
            }

            deferred.resolve(entity);
        });

    } catch (err) {
        console.error(err);
        deferred.reject(new Error(error));
    }

    return deferred.promise;
}
