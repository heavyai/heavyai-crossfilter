/**
 * Wrapper around the connector layer that encapsulates actual query network requests and
 * responses, and how we process it afterwards (e.g. what transformations we apply to it afterwards)
 * This is an associative entity between crossfilter and connection
 */
export default class ResultCache {
  /******************************************************************
   * properties
   */
  _maxCacheSize   = 10
  _cache          = {}
  _cacheCounter   = 0
  _dataConnector  = null
  /***********   CONSTRUCTOR   ***************/
  constructor(dataConnector) {
    this._dataConnector  = dataConnector
    this.initializeQuery = this._initializeQuery
    this.peekAtCache = () => { // haq 4 testing
      return {cache: this._cache, emptyCache: this.emptyCache}
    }
    this._addPublicAPI()
  }
  _addPublicAPI() {
    this.getDataConnector = () => this._dataConnector
    this.setDataConnector = (con) => this._dataConnector = con
    this.getMaxCacheSize  = () => this._maxCacheSize
    this.setMaxCacheSize  = (size) => {
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
    delete this._cache[oldestQuery]
  }
  emptyCache() {
    this._cache = {}
    return this
  }
  processQuery(query, options, callback = null) {
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
      if (numKeys >= _maxCacheSize) {
        this.evictOldestCacheEntry()
      }
    }
    if(async) {
      /** This is where the call is made to connector. It is a great place to inspect a query for proper syntax **/
      return _dataConnector.query(query, conQueryOptions, (error, result) => {
        if (error) {
          callback(error)
        } else {
          this.asyncCallback(query, conQueryOptions.postProcessors, !conQueryOptions.renderSpec, result, conQueryOptions.eliminateNullRows, callback)
        }
      })
    }
    else {
      const result = _dataConnector.query(query, conQueryOptions)
      let data = this.postProcess(result, conQueryOptions.postProcessors)
      if (!conQueryOptions.renderSpec) {
        this._cache[query] = { time: this._cacheCounter++, data: data, showNulls: conQueryOptions.eliminateNullRows || false }
      }
      return data
    }
  }
  query(query, options) {
    return this.processQuery(query, options)
  }
  queryAsync(query, options, callback) {
    return this.processQuery(query, options, callback)
  }
  asyncCallback(query, postProcessors, shouldCache, result, showNulls, callback) {
    let data = this.postProcess(result, postProcessors)

    this._cache[query] = { time: this._cacheCounter++, data: data, showNulls: showNulls }
    callback(null, shouldCache ? this._cache[query].data : data)
  }
  postProcess(result, postProcessors) {
    let data = result
    if(postProcessors) {
      postProcessors.forEach((postProcessor) => {
        data = postProcessor(data)
      })
    }
    return data
  }
}
