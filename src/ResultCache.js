/**
 * Created by andrelockhart on 5/6/17.
 */
/**
 * Wrapper around the connector layer that encapsulates actual query network requests and
 * responses, and how we process it afterwards (e.g. what transformations we apply to it afterwards)
 * This is an associative entity between crossfilter and connection
 */
export default class ResultCache {
    /******************************************************************
     * properties
     */
    _maxCacheSize   = 10 // TODO should be top-level constant or init param
    _cache          = {}
    _cacheCounter   = 0
    /***********   CONSTRUCTOR   ***************/
    constructor(dataConnector) {
        this._dataConnector  = dataConnector // TODO con not used elsewhere
        this.initializeQuery = this._initializeQuery
    }
    /******************************************************************
     * private methods
     */
    _initializeQuery(options) {
        let obj = {}
        if(options) {
            obj.eliminateNullRows   = options.eliminateNullRows ? options.eliminateNullRows : false
            obj.renderSpec          = options.renderSpec ? options.renderSpec : null
            obj.queryId             = options.queryId ? options.queryId : null
            obj.columnarResults     = true
        }
        // debugger
        return obj
    }
    /******************************************************************
     * public methods
     */
    evictOldestCacheEntry() {
        //console.log('ResultCache.evictOldestCacheEntry()')
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
        //console.log('ResultCache.emptyCache()')
        this._cache = {}
        return this
    }
    processQuery(query, options, callback = null) { // todo - simplify more
        //console.log('ResultCache.processQuery()')
        const { _maxCacheSize, _dataConnector } = this
        const numKeys       = Object.keys(this._cache).length,
            async           = !!callback,
            conQueryOptions = this.initializeQuery(options, async)

        if (!conQueryOptions.renderSpec) {

            if (query in this._cache && this._cache[query].showNulls === conQueryOptions.eliminateNullRows) {
                this._cache[query].time = this._cacheCounter++
                if(async) {
                    // change selector to null as it should already be in cache
                    // no postProcessors, shouldCache: true
                    this.asyncCallback(query, false, !conQueryOptions.renderSpec, this._cache[query].data, conQueryOptions.eliminateNullRows, callback)
                    return
                }
                else {
                    return this._cache[query].data
                }
            }
            if (numKeys >= _maxCacheSize) { // should never be gt
                this.evictOldestCacheEntry() // TODO only reachable if query not in cache
            }
        }
        if(async) {
            // todo - confirmed query string matches legacy
            /** This is where the call is made to connector. It is a great place to inspect a query for proper syntax **/
            // debugger
            //console.log('ResultCache.processQuery() - ASYNC value of query: ', query)
            return _dataConnector.query(query, conQueryOptions, (error, result) => {
                if (error) {
                    // debugger
                    //console.log('ResultCache.processQuery() async ERROR')
                    callback(error)
                } else {
                    // debugger
                    //console.log('ResultCache.processQuery() async success')
                    this.asyncCallback(query, options.postProcessors, !conQueryOptions.renderSpec, result, conQueryOptions.eliminateNullRows, callback)
                }
            })
        }
        else {

            let data = this.postProcess(options.postProcessors, _dataConnector.query(query, conQueryOptions))
            //console.log('ResultCache.processQuery() - not async value of data: ', data)
            if (!renderSpec) { // todo tisws ???
                this._cache[query] = { time: this._cacheCounter++, data: data, showNulls: conQueryOptions.eliminateNullRows }
            }
            return data
        }
    }
    query(query, options) {
        //console.log('ResultCache.query()')
        this.processQuery(query, options)
    }
    queryAsync(query, options, callback) {
        //console.log('ResultCache.queryAsync()')
        return this.processQuery(query, options, callback)
    }
    // todo - a lotta params (use conQueryOptions valueObject), & can further simplify
    asyncCallback(query, postProcessors = null, shouldCache, result, showNulls, callback) {
        //console.log('ResultCache.asyncCallback(), post processors: ', postProcessors)
        // todo - confirmed data returned matches legacy
        let data = this.postProcess(result, postProcessors)

        this._cache[query] = { time: this._cacheCounter++, data: data, showNulls: showNulls }
        // debugger
        // this is the callback from ?
        //console.log('ResultCache.asyncCallback - value of cache data: ', this._cache[query].data)
        callback(null, shouldCache ? this._cache[query].data : data)
    }
    postProcess(result, postProcessors) {
        //console.log('ResultCache.postProcess()')
        let data = result
        if(postProcessors) {
            postProcessors.forEach((postProcessor) => {
                data = postProcessor(result)
            })
        }
        return data
    }
}