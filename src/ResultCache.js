/**
 * Created by andrelockhart on 5/6/17.
 */

/**
 * Wrapper around the connector layer that deals with how we make the actual query network request
 * how it responds, and how we process it aftwrwards (e.g. what transformations we apply to it
 * afterwards)
 * This is an associative entity between crossfilter and connection
 */

export default class ResultCache {
    /******************************************************************
     * properties
     */
    _maxCacheSize = 10 // TODO should be top-level constant or init param
    _cache = {}
    _cacheCounter = 0
    /***********   CONSTRUCTOR   ***************/
    constructor(dataConnector) {
        this._dataConnector = dataConnector // TODO con not used elsewhere
    }
    /******************************************************************
     * private methods
     */
    initializeQuery(options, async) {
        let obj = {}
        obj.eliminateNullRows   = options ? options.eliminateNullRows ? options.eliminateNullRows : false : false
        obj.renderSpec          = options ? options.renderSpec ? options.renderSpec : null : null
        obj.queryId             = options ? options.queryId ? options.queryId : null : null
        if(async) {
            obj.columnarResults     = true
        }
        else {
            obj.postProcessors = options ? options.postProcessors ? options.postProcessors : null : null
        }
        return obj
    }
    /******************************************************************
     * public methods
     */
    evictOldestCacheEntry() {
        const { _cache } = this
        let oldestQuery = null,
         lowestCounter  = Number.MAX_SAFE_INTEGER

        Reflect.ownKeys(_cache).map((key) => {
            if (_cache.time < lowestCounter) {
                oldestQuery     = key
                lowestCounter   = _cache.time
            }
        })
        delete _cache[oldestQuery]
    }
    emptyCache() {
        this._cache = {}
        return this
    }
    processQuery(query, options, callback = null) { // todo - simpllfy more
        let { _cache, _cacheCounter, _maxCacheSize } = this,
            numKeys                                  = Reflect(_cache).length
        const async             = !!callback,
              conQueryOptions   = this.initializeQuery(options, async)

        if (!conQueryOptions.renderSpec) {
            if (query in _cache && _cache[query].showNulls === conQueryOptions.eliminateNullRows) {
                _cache[query].time = _cacheCounter++
                if(async) {
                    // change selector to null as it should already be in cache
                    // no postProcessors, shouldCache: true
                    this.asyncCallback(query, null, !conQueryOptions.renderSpec, _cache[query].data, conQueryOptions.eliminateNullRows, callback)
                    return
                }
                else {
                    return _cache[query].data
                }
            }
            if (numKeys >= _maxCacheSize) { // should never be gt
                this.evictOldestCacheEntry() // TODO only reachable if query not in cache
            }
        }
        if(async) {
            return this._dataConnector.query(query, conQueryOptions, (error, result) => {
                if (error) {
                    callback(error)
                } else {
                    this.asyncCallback(query, conQueryOptions.postProcessors, !conQueryOptions.renderSpec, result, conQueryOptions.eliminateNullRows, callback)
                }
            })
        }
        else {
            let data = this.postProcess(postProcessors, _dataConnector.query(query, conQueryOptions))
            if (!renderSpec) { // todo tisws ???
                _cache[query] = { time: _cacheCounter++, data: data, showNulls: conQueryOptions.eliminateNullRows };
            }
            return data
        }
    }
    query(query, options) {
        this.processQuery(query, options)
    }
    queryAsync(query, options, callback) {
        return this.processQuery(query, options, callback)
    }
    // todo - a lotta params (use conQueryOptions valueObject), & can further simplify
    asyncCallback(query, postProcessors, shouldCache, result, showNulls, callback) {
        let { _cache, _cacheCounter } = this
        let data = this.postProcess(result, postProcessors)

        _cache[query] = { time: _cacheCounter++, data: data, showNulls: showNulls }
        callback(null, shouldCache ? cache[query].data : data)
    }
    postProcess(postProcessors = [], result) {
        let data = null
        postProcessors.forEach((postProcessor) => {
            data = postProcessor(result)
        })
        return data
    }
}