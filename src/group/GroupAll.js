/**
 * As opposed to Group, this class is concerned with all columns (e.g. count widget in Immerse)
 * Therefore, this has no notion of 'dimension', e.g. ~'SELECT *'
 */
import ResultCache from "../ResultCache"
import { isRelative, replaceRelative } from "./Filter"

export default class GroupAll {
  /******************************************************************
   * properties
   */
  _reduceExpression = null
  _maxCacheSize = 5
  /***********   CONSTRUCTOR   ***************/
  constructor(dataConnector, crossfilter) {
    this._init(dataConnector, crossfilter)
    this._addPublicAPI()
  }
  _init(dataConnector, crossfilter) {
    this._dataConnector     = dataConnector
    this._cache             = new ResultCache(this._dataConnector)
    this.getCrossfilter     = () => crossfilter
    this.getCrossfilterId   = () => crossfilter.getId()
    this.reduceCount()
  }
  _addPublicAPI() {
    this.reduceMulti = this.reduce
    this.clearResultCache = () => {this._cache = new ResultCache(this._dataConnector)}
  }
  getTable() {
    return this.getCrossfilter().getTable()
  }
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
    return isRelative(filterQuery) ? replaceRelative(filterQuery) : filterQuery
  }
  writeQuery(ignoreFilters, ignoreChartFilters) {
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
    // expressions should be an array of {expression, agg_mode (sql_aggregate), name}
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
    return this
  }
  getReduceExpression() {
    return this._reduceExpression
  }
  value(ignoreFilters, ignoreChartFilters, callback) {
    return this.setValue(ignoreFilters, ignoreChartFilters, callback, true)
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
        "Warning: Deprecated sync method GroupAll.values(). Please use async version"
      )
    }
    let query = this.writeQuery(ignoreFilters, ignoreChartFilters)
    const options = {
      eliminateNullRows   : false,
      renderSpec          : null,
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
    const method    = value ? "value" : "values"
    return new Promise((resolve, reject) => {
      this[method](ignoreFilters, ignoreChartFilters, (error, data) => {
        if (error) {
          reject(error)
        } else {
          resolve(data)
        }
      })
    })
  }
}
