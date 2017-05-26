/**
 * As opposed to Group, this class is concerned with all columns (e.g. count widget in Immerse)
 * Therefore, this has no notion of 'dimension', e.g. ~'SELECT *'
 */
import ResultCache from '../ResultCache'
import { isRelative, replaceRelative } from './Filter'

export default class GroupAll {
    /******************************************************************
     * properties
     */
    _reduceExpression = null
    /***********   CONSTRUCTOR   ***************/
    constructor(dataConnector, crossfilter) {
        // todo - assuming this class is instantiated by another class that holds resultCache, probably CrossFilter?
        //console.log('GroupAll() - constructor')
        this._init(dataConnector, crossfilter)
        this._addPublicAPI()
    }
    _init(dataConnector, crossfilter) {
        this._cache             = new ResultCache(dataConnector)
        this.getCrossfilter     = () => crossfilter
        this.getCrossfilterId   = () => crossfilter.getId()
        this.reduceCount()
    }
    _addPublicAPI() {
        this.reduceMulti = this.reduce
    }
    /******************************************************************
     * private methods
     */
        // todo - tisws from groupAll() under crossfilter
    _maxCacheSize = 5
    /******************************************************************
     * public methods
     */
    writeFilter(ignoreFilters, ignoreChartFilters) {
        const { _filters, _globalFilters } = this.getCrossfilter()
        let filterQuery      = "",
            validFilterCount = 0

        if(!ignoreChartFilters) {
            _filters.forEach((fltr) => {
                if (fltr && fltr !== "") {
                    if (validFilterCount > 0) {
                        filterQuery += " AND "
                    }
                    validFilterCount++
                    filterQuery += fltr
                }
            })
        }
        if(!ignoreFilters) {
            _globalFilters.forEach((globalFilter) => {
                if (globalFilter && globalFilter !== "") {
                    if (validFilterCount > 0) {
                        filterQuery += " AND "
                    }
                    validFilterCount++
                    filterQuery += globalFilter
                }
            })
        }
        //console.log('GroupAll() - writeFilter(), value of filterQuery: ', filterQuery)
        return isRelative(filterQuery) ? replaceRelative(filterQuery) : filterQuery
    }
    writeQuery(ignoreFilters, ignoreChartFilters) {
        console.log('GroupAll.writeQuery()')
        const { _tablesStmt, _joinStmt } = this.getCrossfilter()
        let query       = "SELECT " + this._reduceExpression + " FROM " + _tablesStmt,
            filterQuery = this.writeFilter(ignoreFilters, ignoreChartFilters)
        if (filterQuery !== "") {
            query += " WHERE " + filterQuery
        }
        if (_joinStmt !== null) {
            if (filterQuery === "") {
                query += " WHERE "
            } else {
                query += " AND "
            }
            query += _joinStmt
        }
        if (_joinStmt !== null) {
            query += " WHERE " + _joinStmt
        }
        // debugger
        // could use alias "key" here
        //console.log('GroupAll() - writeQuery(), value of query: ', query)
        return query
    }
    reduceCount(countExpression, name) {
        if (typeof countExpression !== "undefined")
            this._reduceExpression  = "COUNT(" + countExpression + ") as " + (name || "val")
        else
            this._reduceExpression  = "COUNT(*) as val"
        //console.log('GroupAll() - reduceCount(), countExpression: ', countExpression)
        //console.log('GroupAll() - reduceCount(), name: ', name)
        //console.log('GroupAll() - reduceCount(), _reduceExpression: ', this._reduceExpression)
        //console.log('GroupAll() - reduceCount(), this: ', this)
        return this
    }
    reduceSum(sumExpression, name) {
        this._reduceExpression  = "SUM(" + sumExpression + ") as " + (name || "val")
        //console.log('GroupAll() - reduceSum()')
        return this
    }
    reduceAvg(avgExpression, name) {
        this._reduceExpression = "AVG(" + avgExpression + ") as " + (name || "val")
        //console.log('GroupAll() - reduceAvg()')
        return this
    }
    reduceMin(minExpression, name) {
        this._reduceExpression = "MIN(" + minExpression + ") as " + (name || "val")
        //console.log('GroupAll() - reduceMin()')
        return this
    }
    reduceMax(maxExpression, name) {
        this._reduceExpression = "MAX(" + maxExpression + ") as " + (name || "val")
        //console.log('GroupAll() - reduceMax()')
        return this
    }
    reduce(expressions) {
        //expressions should be an array of {expression, agg_mode (sql_aggregate), name}
        this._reduceExpression = ""
        expressions.forEach((expression, i) => {
            if (i > 0) {
                this._reduceExpression += ","
            }
            let agg_mode = expression.agg_mode.toUpperCase()

            if (agg_mode === "CUSTOM") {
                this._reduceExpression += expression.expression
            }
            else if (agg_mode === "COUNT") {
                if (typeof expression.expression !== "undefined") {
                    this._reduceExpression += "COUNT(" + expression.expression + ")"
                } else {
                    this._reduceExpression += "COUNT(*)"
                }
            }
            else { // should check for either sum, avg, min, max
                this._reduceExpression += agg_mode + "(" + expression.expression + ")"
            }
            this._reduceExpression += " AS " + expression.name
        })
        //console.log('GroupAll() - reduce()')
        return this
    }
    value(ignoreFilters, ignoreChartFilters, callback) {
        console.log('GroupAll() - value()')
        return this.setValue(ignoreFilters, ignoreChartFilters, callback, true)
    }
    valueAsync(ignoreFilters = false, ignoreChartFilters = false) {
        //console.log('GroupAll() - valueAsync()')
        return this.getValuePromise(ignoreFilters, ignoreChartFilters, true)
    }
    values(ignoreFilters, ignoreChartFilters, callback) {
        console.log('GroupAll() - values()')
        return this.setValue(ignoreFilters, ignoreChartFilters, callback)
    }
    valuesAsync(ignoreFilters = false, ignoreChartFilters = false) {
        //console.log('GroupAll() - valuesAsync()')
        return this.getValuePromise(ignoreFilters, ignoreChartFilters)
    }
    setValue(ignoreFilters, ignoreChartFilters, callback, value = false) {
        console.log('GroupAll() - setValue()')
        const { _cache } = this
        if (!callback) {
            console.warn(
                "Warning: Deprecated sync method GroupAll.values(). Please use async version"
            )
        }
        let query = this.writeQuery(ignoreFilters, ignoreChartFilters)
        const options = {
            eliminateNullRows   : false,
            renderSpec          : null,
            // todo - this logic is slightly different to old cf (handles multiple renderAllAsync() calls
            postProcessors      : value ? [(d) => Array.isArray(d) ? d[0].val : d]
                                        : [(d) => Array.isArray(d) ? d[0] : null],
            queryId             : -1
        }
        if (callback) {
            return _cache.queryAsync(query, options, callback)
        } else {
            return _cache.query(query, options)
        }
    }
    getValuePromise(ignoreFilters = false, ignoreChartFilters = false, value = false) {
        const method    = value ? 'value' : 'values'
        //console.log('GroupAll() - getValuePromise(), value of method: ', method)
        return new Promise((resolve, reject) => {
            this[method](ignoreFilters, ignoreChartFilters, (error, data) => {
                if (error) {
                    //console.log('>>>>>>>>>> >>>>>>>>>>>>   GroupAll() - valuesAsync() Promise: ERROR')
                    reject(error)
                } else {
                    //console.log('>>>>>>>>>> >>>>>>>>>>>>   GroupAll() - valuesAsync() Promise: resolve, value of data: ', data)
                    resolve(data)
                }
            })
        })
    }
}