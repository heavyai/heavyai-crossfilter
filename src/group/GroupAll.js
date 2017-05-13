/**
 * Created by andrelockhart on 5/6/17.
 */

/**
 * As opposed to Group, this class is concerned with all columns (e.g. count widget in Immerse)
 */
import ResultCache from '../ResultCache'
import { replaceRelative } from './Filter'

export default class GroupAll {
    /******************************************************************
     * properties
     */
    _reduceExpression = null
    /***********   CONSTRUCTOR   ***************/
    constructor(dataConnector) {
        // todo - assuming this class is instantiated by another class that holds resultCache, probably CrossFilter?
        this._init(dataConnector, dimension)
    }
    _init(dataConnector, dimension) {
        // make dimension instance available to instance
        this.getDimension = () => dimension
        this._cache = new ResultCache(dataConnector)
    }
    /******************************************************************
     * private methods
     */
        // todo - tisws from groupAll() under crossfilter
    _maxCacheSize = 5
    /******************************************************************
     * public methods
     */
    writeFilter(filters, globalFilters, ignoreFilters, ignoreChartFilters) {
        let filterQuery      = "",
            validFilterCount = 0

        if(!ignoreChartFilters) {
            filters.forEach((fltr) => {
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
            globalFilters.forEach((globalFilter) => {
                if (globalFilter && globalFilter !== "") {
                    if (validFilterCount > 0) {
                        filterQuery += " AND "
                    }
                    validFilterCount++
                    filterQuery += globalFilter
                }
            })
        }
        return isRelative(filterQuery) ? replaceRelative(filterQuery) : filterQuery;
    }
    writeQuery(ignoreFilters, ignoreChartFilters) {
        const { _tablesStmt, _joinStmt } = this.getDimension().getCrossfilter()
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
        // could use alias "key" here
        return query
    }
    reduceCount(countExpression, name) {
        if (typeof countExpression !== "undefined")
            this._reduceExpression  = "COUNT(" + countExpression + ") as " + (name || "val")
        else
            this._reduceExpression  = "COUNT(*) as val"
        return this
    }
    reduceSum(sumExpression, name) {
        this._reduceExpression  = "SUM(" + sumExpression + ") as " + (name || "val")
        return this
    }
    reduceAvg(avgExpression, name) {
        this._reduceExpression = "AVG(" + avgExpression + ") as " + (name || "val")
        return this
    }
    reduceMin(minExpression, name) {
        this._reduceExpression = "MIN(" + minExpression + ") as " + (name || "val")
        return this
    }
    reduceMax(maxExpression, name) {
        this._reduceExpression = "MAX(" + maxExpression + ") as " + (name || "val")
        return this
    }
    reduce(expressions) {
        let { _reduceExpression } = this
        //expressions should be an array of {expression, agg_mode (sql_aggregate), name}
        _reduceExpression = ""
        expressions.forEach((expression, i) => {
            if (i > 0) {
                _reduceExpression += ","
            }
            let agg_mode = expression.agg_mode.toUpperCase()

            if (agg_mode === "CUSTOM") {
                _reduceExpression += expression.expression
            }
            else if (agg_mode === "COUNT") {
                if (typeof expression.expression !== "undefined") {
                    _reduceExpression += "COUNT(" + expression.expression + ")"
                } else {
                    _reduceExpression += "COUNT(*)"
                }
            }
            else { // should check for either sum, avg, min, max
                _reduceExpression += agg_mode + "(" + expression.expression + ")"
            }
            _reduceExpression += " AS " + expression.name
        })
        return this
    }
    value(dimension, ignoreFilters, ignoreChartFilters, callback) {
        return this.setValue(dimension, ignoreFilters, ignoreChartFilters, callback, true)
    }
    valueAsync(ignoreFilters = false, ignoreChartFilters = false) {
        return this.getValuePromise(ignoreFilters, ignoreChartFilters, true)
    }
    values(ignoreFilters, ignoreChartFilters, callback) {
        return this.setValue(ignoreFilters, ignoreChartFilters, callback)
    }
    valuesAsync(ignoreFilters = false, ignoreChartFilters = false) {
        return this.getValuePromise(ignoreFilters, ignoreChartFilters)
    }
    setValue(ignoreFilters, ignoreChartFilters, callback, value = false) {
        const { _cache } = this
        if (!callback) {
            console.warn(
                "Warning: Deprecated sync method groupAll.values(). Please use async version"
            )
        }
        let query = this.writeQuery(ignoreFilters, ignoreChartFilters)
        const options = {
            eliminateNullRows   : false,
            renderSpec          : null,
            postProcessors      : value ? [function (d) {return d[0].val}] : [function (d) {return d[0]}],
            queryId             : -1
        }
        if (callback) {
            return _cache.queryAsync(query, options, callback)
        } else {
            return _cache.query(query, options)
        }
    }
    getValuePromise(ignoreFilters = false, ignoreChartFilters = false, value = false) {
        const dimension = this.getDimension(),
              method    = value ? 'value' : 'values'
        return new Promise((resolve, reject) => {
            this[method](dimension, ignoreFilters, ignoreChartFilters, (error, data) => {
                if (error) {
                    reject(error)
                } else {
                    resolve(data)
                }
            })
        })
    }
}