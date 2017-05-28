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
    _dataConnector  = null
    /***********   CONSTRUCTOR   ***************/
    constructor(dataConnector) {
        this._dataConnector  = dataConnector // TODO con not used elsewhere
        this.initializeQuery = this._initializeQuery
        // this.postProcess = this._postProcess
        this.peekAtCache = () => { // haq 4 testing
            // console.log('ResultCache.peekAtCache() - value of _cache: ', this._cache)
            return {cache: this._cache, emptyCache: this.emptyCache}
        }
        this._addPublicAPI()
    }
    _addPublicAPI() {
        this.getDataConnector = () => this._dataConnector
        this.setDataConnector = (con) => this._dataConnector = con
        this.getMaxCacheSize  = () => this._maxCacheSize
        this.setMaxCacheSize  = (size) => {
            // console.log('ResultCache.setMaxCacheSize() - size: ', size)
            this._maxCacheSize = size
        }
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
            obj.postProcessors      = options.postProcessors ? options.postProcessors : null
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
            if (_cache[key].time < lowestCounter) {
                oldestQuery     = key
                lowestCounter   = _cache[key].time
            }
        })
        // console.log('<><><><><> <><><><><>   ResultCache.evictOldestCacheEntry() - oldestQuery: ', oldestQuery)
        delete this._cache[oldestQuery]
    }
    emptyCache() {
        // console.log('ResultCache.emptyCache()')
        this._cache = {}
        return this
    }
    processQuery(query, options, callback = null) { // todo - simplify more
        const { _maxCacheSize, _dataConnector } = this
        const numKeys       = Object.keys(this._cache).length,
            async           = !!callback,
            conQueryOptions = this.initializeQuery(options, async)

        // console.log('ResultCache.processQuery() - conQueryOptions.renderSpec: ' + conQueryOptions.renderSpec + ' async: ' + async)
        if (!conQueryOptions.renderSpec) {

            if (query in this._cache && this._cache[query].showNulls === conQueryOptions.eliminateNullRows) {
                this._cache[query].time = this._cacheCounter++
                if(async) {
                    // change selector to null as it should already be in cache
                    // no postProcessors, shouldCache: true
                    // console.log('resultCache.query - !renderSpec && query in cache && async')
                    this.asyncCallback(query, false, !conQueryOptions.renderSpec, this._cache[query].data, conQueryOptions.eliminateNullRows, callback)
                    return
                }
                else {
                    console.log('resultCache.query - !renderSpec && query in cache && not async, fetching from cache')
                    return this._cache[query].data
                }
            }
            if (numKeys >= _maxCacheSize) { // should never be gt
                // console.log('ResultCache.processQuery - evicting cache !!!, _maxCacheSize: ', _maxCacheSize)
                this.evictOldestCacheEntry() // TODO only reachable if query not in cache
            }
        }
        if(async) {
            // todo - confirmed query string matches legacy
            /** This is where the call is made to connector. It is a great place to inspect a query for proper syntax **/
            // debugger
            //console.log('ResultCache.processQuery() - making call to _dataConnector, ASYNC value of query: ', query)
            return _dataConnector.query(query, conQueryOptions, (error, result) => {
                if (error) {
                    // debugger
                    console.log('ResultCache.processQuery() async ERROR')
                    callback(error)
                } else {
                    // debugger
                    console.log('ResultCache.processQuery() async success')
                    this.asyncCallback(query, conQueryOptions.postProcessors, !conQueryOptions.renderSpec, result, conQueryOptions.eliminateNullRows, callback)
                }
            })
        }
        else {
            const result = _dataConnector.query(query, conQueryOptions)
            let data = this.postProcess(result, conQueryOptions.postProcessors)
            // console.log('ResultCache.processQuery() - not async value of data: ', data)
            if (!conQueryOptions.renderSpec) { // todo tisws ???
                this._cache[query] = { time: this._cacheCounter++, data: data, showNulls: conQueryOptions.eliminateNullRows || false }
            }
            //console.log('ResultCache.query - not async, value of data: ', data)
            return data
        }
    }
    query(query, options) {
        // console.log('ResultCache.query()')
        return this.processQuery(query, options)
    }
    queryAsync(query, options, callback) {
        //console.log('ResultCache.queryAsync()')
        return this.processQuery(query, options, callback)
    }
    // todo - a lotta params (use conQueryOptions valueObject), & can further simplify
    asyncCallback(query, postProcessors, shouldCache, result, showNulls, callback) {
        // console.log('ResultCache.asyncCallback(), post processors: ', postProcessors)
        //console.log('ResultCache.asyncCallback(), shouldCache: ', shouldCache)
        // todo - confirmed data returned matches legacy
        let data = this.postProcess(result, postProcessors)

        this._cache[query] = { time: this._cacheCounter++, data: data, showNulls: showNulls }
        // this is the callback from ?
        //console.log('ResultCache.asyncCallback - value of cache data: ', this._cache[query].data)
        callback(null, shouldCache ? this._cache[query].data : data)
    }
    postProcess(result, postProcessors) {
        // console.log('=============== ================== >>>   ResultCache.postProcess() - value of result: ', result)
        // console.log('ResultCache.postProcess() - value of postProcessors: ', postProcessors)
        // console.log('ResultCache.postProcess() - value of result: ', result)
        // debugger
        let data = result
        if(postProcessors) {
            postProcessors.forEach((postProcessor) => {
                data = postProcessor(data)
            })
        }
        return data
    }
}