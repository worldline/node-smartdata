var request = require('request');
var querystring = require('querystring');

SmartData = function (username, password) {
    var outApi = "http://out.api.v2.smartdata.io";
    var inApi = "http://in.api.v2.smartdata.io";
    var username = username;
    var password = password;

    var _methods = ['GET', 'POST', 'PUT', 'DELETE'];

    /**
     * Requests SmartData's "out" api
     * @param method HTTP method to use
     * @param path API path to request
     * @param params JSON parameters passed to the api request
     * @param cb Callback function
     * @private
     */
    _out = function (path, params, method, cb) {
        _resolveAndFetch(outApi, path, params, method, cb);
    };

    /**
     * Requests SmartData's "in" api
     * @param method HTTP method to use
     * @param path API path to request
     * @param params JSON parameters passed to the api request
     * @param cb Callback function = function(response, body)
     * @private
     */
    _in = function (path, params, method, cb) {
        _resolveAndFetch(inApi, path, params, method, cb);
    };

    var _resolveAndFetch = function (api, path, params, method, cb) {
        if (typeof cb === 'undefined') {
            cb = method;
            // check if params actually contains a method
            if (typeof params === 'string' && _methods.indexOf(params.toUpperCase()) > -1) {
                method = params;
                params = null;
            } else {
                method = null;
            }
        }
        _fetch(method === null ? 'GET' : method, api, path, params, cb);
    }

    var _fetch = function (method, url, path, params, cb) {
        if (typeof cb === "undefined") {
            // no params
            cb = params;
            params = null;
        }
        var options = {};
        var paramsString = "";
        if (params !== null) {
            if (method === 'GET') {
                // Params present. If method is GET, convert to querystring
                paramsString = "?" + querystring.stringify(params);
            } else {
                // Otherwise, set params as json attribute
                options['json'] = params;
            }
        }
        options['uri'] = url + path + paramsString;
        options['port'] = 80;
        options['method'] = method.toUpperCase();
        options['proxy'] = "http://example.com:8080";
        options['headers'] = {login: username, password: password}

        request(options, function (error, response, body) {
            if (error !== undefined && error !== null) {
                console.error(error);
            } else {
                cb(response, body);
            }
        });
    };

    /**
     * Invoke callback and print warning if it was undefined
     * @private
     */
    _invoke = function () {
        var args = [];
        for (var i = 0; i < arguments.length; i++) {
            args.push(arguments[i]);
        }

        var cb = args.shift();
        if (cb === undefined) {
            console.warn("Undefined callback, results are unused");
        } else {
            cb.apply(this, args);
        }
    }
};

/**
 * List the streams with pagination
 */
SmartData.prototype.streams = function (params, cb) {
    _in('/stream/', params, function (response, body) {
        var streams = JSON.parse(body);
        wrappedStreams = [];
        for (streamIndex in streams.results) {
            wrappedStreams.push(new DataStream(streams.results[streamIndex]));
        }
        _invoke(cb, wrappedStreams);
    });
}

/**
 * Get specific stream by id
 * @param streamId Stream's id
 * @param cb callback = function(stream)
 */
SmartData.prototype.stream = function (streamId, cb) {
    _in('/stream/' + streamId, function (response, body) {
        var streamWrapper = new DataStream(JSON.parse(body));
        _invoke(cb, streamWrapper);
    });
};

/**
 * List the sources with pagination
 */
SmartData.prototype.sources = function (params, cb) {
    if(typeof cb === 'undefined') {
        cb = params;
        params = null;
    }
    _in('/source/', params, function (response, body) {
        var sources = JSON.parse(body);
        wrappedSources = [];
        for (sourceIndex in sources.results) {
            wrappedSources.push(new DataSource(sources.results[sourceIndex]));
        }
        _invoke(cb, wrappedSources);
    });
}

/**
 * Get the source handle for given id
 *
 * @param sourceId a datasource id
 * @param cb callback = function(source)
 */
SmartData.prototype.source = function (sourceId, cb) {
    _in('/source/' + sourceid, function (response, body) {
        var sourceWrapper = new DataSource(JSON.parse(body));
        _invoke(cb, sourceWrapper);
    });
};

/**
 * Initialize DataStream wrapper from existing base. Base must at least contain an _id attribute
 * @param base Base object, must contain an _id attribute with the id of the stream
 * @constructor
 */
DataStream = function (base) {
    for (var prop in base) {
        if (base.hasOwnProperty(prop)) {
            this[prop] = base[prop];
        }
    }
};

/**
 * Fetch data from stream
 * @param params query      Filter query
 *               from       Rank (0-based) of the first returned result to return
 *               size       Number of results to returned
 *               sortOrder  Sorting order, asc or desc
 *               sortField  Path to the field used to sort results
 *               sortAField Path to the field used to sort result, lexicographically
 *               sortNField Path to the field used to sort result, numerically
 *               token      Security token used for external api authentication
 * @param cb callback = function(data)
 */
DataStream.prototype.data = function (params, cb) {
    _out('/' + this._id, params, function (response, body) {
        _invoke(cb, JSON.parse(body));
    });
};

/**
 * Computes a list of the stream's fields
 * @param cb callback = function(fields)
 */
DataStream.prototype.fields = function (cb) {
    _out('/' + this._id + '/fields', function (response, body) {
        var fields = JSON.parse(body).fields.content;
        _invoke(cb, fields);
    });
};

/**
 * Fetch data from stream
 * @param params field      Field where distinct values of the stream are requested
 *               query      Find elements matching a query
 * @param cb callback = function(values)
 */
DataStream.prototype.values = function (params, cb) {
    _out('/' + this._id + '/values', params, function (response, body) {
        var values = JSON.parse(body).content;
        _invoke(cb, values);
    })
};

/**
 * DataSource wrapper for SmartData datasources
 * @param base Base object, must contain an _id attribute with the id of the source
 * @constructor
 */
DataSource = function (base) {
    for (var prop in base) {
        if (base.hasOwnProperty(prop)) {
            this[prop] = base[prop];
        }
    }
};

/**
 * Update a datasource
 * @param data new datasource info
 * @param cb callback = function(updatedSource)
 */
DataSource.prototype.update = function (data, cb) {
    _in('PUT', '/source/' + this._id, function (response, body) {
        var sourceWrapper = new DataSource(JSON.parse(body));
        _invoke(cb, sourceWrapper);
    });
};

/**
 * Delete datasource
 * @param cb callback = function()
 */
DataSource.prototype.delete = function (cb) {
    _in('DELETE', '/source/' + this._id, function (response, body) {
        _invoke(cb);
    })
};

/**
 * List every possible leaf of every object contained in the given datasource
 * @param cb callback = function(fields)
 */
DataSource.prototype.fields = function (cb) {
    _in('/source/' + this._id, function (response, body) {
        var fields = JSON.parse(body).fields;
        _invoke(cb, fields);
    })
};

/**
 * Preview the application of a given mapping on a datasource. It's only a preview: the datasource is not modified
 *
 * @param realPath Original name
 * @param mappedTo A better name
 * @param cb Callback = function(mapping)
 */
DataSource.prototype.mappingPreview = function (realPath, mappedTo, cb) {
    _in('POST', '/source/' + this._id + '/mapping-preview', {'realPath': realPath, 'mappedTo': mappedTo}, function(response, body) {
        _invoke(cb, body);
    })
};

/**
 * Integrate data in source
 *
 * @param data either a path to a file containing the data, or plain data
 */
DataSource.prototype.put = function (data) {
    // TODO
};

/**
 * Triggers integration of the datasource
 */
DataSource.prototype.integrate = function () {
    // TODO
};

/**
 * Remove data in the source
 *
 * @param query remove documents matching this query
 */
DataSource.prototype.remove = function (query) {
    // TODO
};

module.exports = SmartData;
