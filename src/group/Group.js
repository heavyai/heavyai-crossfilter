/**
 * Group: A child of the dimension that receives state/context from the dimension
 * so it knows what table it's in, what filters it's using, etc. so it can write a query
 * state that group is concerned with is aggregated
 * It also is concerned with how data is binned, temporal binning is treated
 * differently than quantitative binning
 * For example, in Immerse, Measures are used as part of the grouping query (aggregate)
 *  THIS IS WHAT THE CHARTING LIBRARY USES TO WRITE QUERIES AND MAKE REQUESTS
 */
import ResultCache from "../ResultCache"
import fillBins from "./Bins"
import { unBinResults } from "../modules/binning"
import {sizeAsyncWithEffects, sizeSyncWithEffects} from "./group-utilities"
import { type, writeGroupFilter } from "./Filter"

function uncast (string) {
  const matching = string.match(/^CAST\([a-z,_]{0,250}/)
  if (matching) {
    return matching[0].split("CAST(")[1]
  } else {
    return string
  }
}

export default class Group {
  /******************************************************************
   * properties
   */
  type = "group"
  _reduceExpression       = null  // count will become default
  _reduceSubExpressions   = null
  _reduceVars             = null
  _boundByFilter          = false
  _lastTargetFilter       = null
  _targetSlot             = 0
  _fillMissingBins        = false
  _orderExpression        = null
  _binParams              = []
  binParams(binParamsIn) {
    if (!arguments.length) {
      return this._binParams
    }
    this._binParams = binParamsIn
    return this
  }
  /***********   CONSTRUCTOR   ***************/
  // legacy params: none
  constructor(dataConnector, dimension) {
    this._init(dimension)
    this._addPublicAPI(dimension)
  }
  /***********   INITIALIZATION   ***************/
  _init(dimension) {

    this._dataConnector = dimension.getCrossfilter()._dataConnector
    // make dimension instance available to instance
    this.getDimension   = () => dimension
    this._cache         = new ResultCache(this._dataConnector)
    dimension._dimensionGroups.push(this)
    this.writeFilter    = (queryBinParams) => writeGroupFilter(queryBinParams, this)
    this.reduceCount()
  }
  _addPublicAPI(dimension) {
    this.bottomAsync            = this.bottom
    this.reduceMulti            = this.reduce
    this.allAsync               = this.all
    this.getMinMaxWithFilters   = this.minMaxWithFilters
    this.getProjectOn           = this.buildProjectExpressions
    this.getCrossfilter         = () => dimension.getCrossfilter()
    this.getCrossfilterId       = () => dimension.getCrossfilterId()
    this.getTable               = () => dimension.getTable()
    this.getReduceExpression    = () => this._reduceExpression
    this.setBinParams           = this.binParams
    this.dimension              = () => dimension
    this.clearResultCache = () => this._cache = new ResultCache(this._dataConnector)
  }
  _getDataConnector() {
    return this.getCrossfilter()._dataConnector
  }
  /******************************************************************
   * public methods
   */
  setTargetSlot(s) {
    this._targetSlot = s
  }
  getTargetSlot() {
    return this._targetSlot
  }
  buildProjectExpressions(isRenderQuery, queryBinParams) {
    let { _boundByFilter, _binParams, _reduceExpression } = this,
      { _dimArray, _rangeFilters, _dimContainsArray }     = this.getDimension(),
      projectExpressions                                  = []

    for (let d = 0; d < _dimArray.length; d++) {
      // tableSet[columnTypeMap[dimArray[d]].table] =
      //   (tableSet[columnTypeMap[dimArray[d]].table] || 0) + 1;
      if (queryBinParams !== null
        && queryBinParams !== undefined
        && typeof queryBinParams[d] !== "undefined"
        && queryBinParams[d] !== null) {

        let binBounds = _boundByFilter
        && typeof _rangeFilters[d] !== "undefined"
        && _rangeFilters[d] !== null ? _rangeFilters[d] : queryBinParams[d].binBounds

        let binnedExpression = this.getBinnedDimExpression(
          _dimArray[d],
          binBounds,
          queryBinParams[d].numBins,
          queryBinParams[d].timeBin,
          queryBinParams[d].extract
        )
        projectExpressions.push(binnedExpression + " as key" + d.toString())
      }
      else if (_dimContainsArray[d]) {
        projectExpressions.push("UNNEST(" + _dimArray[d] + ")" + " as key" + d.toString())
      }
      else if (_binParams && _binParams[d]) {
        let binnedExpression = this.getBinnedDimExpression(
          _dimArray[d],
          _binParams[d].binBounds,
          _binParams[d].numBins,
          _binParams[d].timeBin,
          _binParams[d].extract
        )
        projectExpressions.push(binnedExpression + " as key" + d.toString())
      }
      else {
        if (!!isRenderQuery && _dimArray[d].match(/rowid\s*$/)) {
          // do not cast rowid with 'as key[0-9]' as that will mess up hit-test renders
          // and poly renders.
          projectExpressions.push(_dimArray[d])
        } else {
          projectExpressions.push(_dimArray[d] + " as key" + d.toString())
        }
      }
    }
    if (_reduceExpression) {
      projectExpressions.push(_reduceExpression)
    }
    return projectExpressions
  }
  getBinnedDimExpression(expression, binBounds, numBins = 0, timeBin, extract) { // jscs:ignore maximumLineLength
    let isDate = type(binBounds[0]) === "date"
    if (isDate) {
      if (timeBin) {
        if (!!extract) {
          return "extract(" + timeBin + " from " + uncast(expression) + ")"
        } else {
          return "date_trunc(" + timeBin + ", " + expression + ")"
        }
      } else {
        const dimExpr = "extract(epoch from " + expression + ")",
          // as javscript epoch is in ms
          filterRange     = (binBounds[1].getTime() - binBounds[0].getTime()) * 0.001,
          binsPerUnit     = (numBins / filterRange),
          lowerBoundsUTC  = binBounds[0].getTime() / 1000
        return "cast(" +
          "(" + dimExpr + " - " + lowerBoundsUTC + ") * " + binsPerUnit + " as int)"
      }
    } else {
      let filterRange = binBounds[1] - binBounds[0],
        binsPerUnit   = (numBins / filterRange)

      if (filterRange === 0) {
        binsPerUnit = 0
      }
      return "cast(" +
        "(cast(" + expression + " as float) - " + binBounds[0] + ") * " + binsPerUnit + " as int)"
    }
  }
  /******************************************************************
   * query writing
   */
  writeQuery(queryBinParams, sortByValue, ignoreFilters, hasRenderSpec) {
    let query = "SELECT "
    const dimension                                   = this.getDimension(),
      { _targetFilter, _tablesStmt, _joinStmt }       = dimension.getCrossfilter(),
      { _allowTargeted, _dimArray, _eliminateNull }   = dimension
    if (this._reduceSubExpressions
      && (_allowTargeted && (_targetFilter !== null || _targetFilter !== this._lastTargetFilter))) {
      this.reduce(this._reduceSubExpressions)
      this._lastTargetFilter = _targetFilter
    }
    let projectExpressions = this.buildProjectExpressions(hasRenderSpec, queryBinParams)
    if (!projectExpressions) {
      return ""
    }
    query += projectExpressions.join(",")
    query += checkForSortByAllRows() + " FROM " + _tablesStmt
    function checkForSortByAllRows() {
      return sortByValue === "countval" ? ", COUNT(*) AS countval" : ""
    }
    let filterQuery = ignoreFilters ? "" : this.writeFilter(queryBinParams)
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
    // could use alias "key" here
    query += " GROUP BY "
    _dimArray.forEach((dim, i) => {
      if (i !== 0) {
        query += ", "
      }
      if (!!hasRenderSpec && dim.match(/rowid\s*$/)) {
        // do not cast rowid with 'as key[0-9]'
        // as that will mess up hit-test renders
        // and poly renders.
        query += dim
      } else {
        query += "key" + i.toString()
      }
    })
    if (queryBinParams !== null) {
      let havingClause = " HAVING ",
        hasBinParams   = false

      queryBinParams.forEach((queryBinParam, i) => {
        if (queryBinParam !== null && !queryBinParam.timeBin) {
          let havingSubClause = ""
          if (i > 0 && hasBinParams) {
            havingClause += " AND "
          }
          hasBinParams = true
          havingSubClause += "key" + i.toString() + " >= 0 AND key" +
            i.toString() + " < " + queryBinParam.numBins
          if (!_eliminateNull) {
            havingSubClause = `(${havingSubClause} OR key${i.toString()} IS NULL)`
          }
          havingClause += havingSubClause
        }
      })
      if (hasBinParams) {
        query += havingClause
      }
    }
    return query
  }
  setBoundByFilter(boundByFilterIn) {
    this._boundByFilter = boundByFilterIn
    return this
  }
  order(orderExpression) {
    this._orderExpression = orderExpression
    return this
  }
  orderNatural() {
    this._orderExpression = null
    return this
  }
  all(callback) {
    const me = this,
      { _cache } = this,
      { _dimArray, _eliminateNull, _dimensionIndex } = this.getDimension()
    if (!callback) {
      console.warn("Warning: Deprecated sync method group.all(). Please use async version")
    }
    // freeze bin params so they don't change out from under us
    let queryBinParams = this.binParams()
    if (!queryBinParams.length) {
      queryBinParams = null
    }
    let query = this.writeQuery(queryBinParams)
    query += " ORDER BY "
    _dimArray.forEach((dim, i) => {
      if (i > 0) query += ","
      query += "key" + i.toString()
    })
    const postProcessors = [
      function unBinResultsForAll(results) {
        if (queryBinParams) {
          const filledResults = fillBins(me, queryBinParams, results)

          return unBinResults(queryBinParams, filledResults)
        } else {
          return results
        }
      }
    ]
    const options = {
      eliminateNullRows   : _eliminateNull,
      renderSpec          : null,
      postProcessors      : postProcessors,
      queryId             : _dimensionIndex
    }
    if (callback) {
      return _cache.queryAsync(query, options, callback)
    } else {
      return _cache.query(query, options)
    }
  }
  minMaxWithFilters({min = "min_val", max = "max_val"} = {}) {
    const { _cache } = this,
      dimension                       = this.getDimension(),
      { _tablesStmt }                 = dimension.getCrossfilter(),
      { _dimArray, _eliminateNull }   = dimension,
      filters                         = this.writeFilter(),
      filterQ                         = filters.length ? `WHERE ${filters}` : "",
      query                           = `SELECT MIN(${_dimArray[0]}) as ${min}, MAX(${_dimArray[0]}) as ${max} FROM ${_tablesStmt} ${filterQ}`,
      options = {
        eliminateNullRows   : _eliminateNull,
        postProcessors      : [(d) => d[0]],
        renderSpec          : null,
        queryId             : -1
      }
    return new Promise((resolve, reject) => {
      _cache.queryAsync(query, options, (error, val) => {
        error ? reject(error) : resolve(val)
      })
    })
  }
  writeTopBottomQuery(k, offset, ascDescExpr, ignoreFilters, isRender) {
    const { _orderExpression, _reduceVars } = this,
      queryBinParams                        = this.binParams()
    let query = this.writeQuery((queryBinParams.length ? queryBinParams : null), _orderExpression, ignoreFilters, !!isRender)

    if (!query) {
      return ""
    }

    query += " ORDER BY "
    if (_orderExpression) {
      query += _orderExpression + ascDescExpr
    } else {
      let reduceArray = _reduceVars.split(","),
        reduceSize  = reduceArray.length
      for (let r = 0; r < reduceSize - 1; r++) {
        query += reduceArray[r] + ascDescExpr + ","
      }
      query += reduceArray[reduceSize - 1] + ascDescExpr
    }
    if (k !== Infinity) {
      query += " LIMIT " + k
    }
    if (offset !== undefined)
      query += " OFFSET " + offset
    return query
  }
  writeTopQuery(k, offset, ignoreFilters, isRender) {
    return this.writeTopBottomQuery(k, offset, " DESC", ignoreFilters, isRender)
  }
  top(k, offset, renderSpec, callback, ignoreFilters) {
    const { _cache } = this,
      { _eliminateNull, _dimensionIndex } = this.getDimension()
    if (!callback) {
      console.warn("Warning: Deprecated sync method group.top(). Please use async version")
    }
    // freeze bin params so they don't change out from under us
    let queryBinParams = this.binParams()
    if (!queryBinParams.length) {
      queryBinParams = null
    }
    const query = this.writeTopQuery(k, offset, ignoreFilters, !!renderSpec),
      postProcessors = [
        function unBinResultsForTop(results) {
          if (queryBinParams) {
            return unBinResults(queryBinParams, results)
          } else {
            return results
          }
        },
      ],
      options = {
        eliminateNullRows: _eliminateNull,
        renderSpec: renderSpec,
        postProcessors: postProcessors,
        queryId: _dimensionIndex
      }

    if (callback) {
      return _cache.queryAsync(query, options, callback)
    } else {
      return _cache.query(query, options)
    }
  }
  topAsync (k, offset, renderSpec, ignoreFilters) {
    return new Promise((resolve, reject) => {
      this.top(k, offset, renderSpec, (error, result) => {
        if (error) {
          reject(error)
        } else {
          resolve(result)
        }
      }, ignoreFilters)
    })
  }
  writeBottomQuery(k, offset, ignoreFilters, isRender) {
    return this.writeTopBottomQuery(k, offset, "", ignoreFilters, isRender)
  }
  bottom(k, offset, renderSpec, callback, ignoreFilters) {
    const { _cache } = this,
      { _eliminateNull, _dimensionIndex } = this.getDimension()
    if (!callback) {
      console.warn(
        "Warning: Deprecated sync method group.bottom(). Please use async version"
      )
    }
    // freeze bin params so they don't change out from under us
    let queryBinParams = this.binParams()
    if (!queryBinParams.length) {
      queryBinParams = null;
    }
    const query = this.writeBottomQuery(k, offset, ignoreFilters, !!renderSpec),
      postProcessors = [
        function unBinResultsForBottom(results) {
          if (queryBinParams) {
            return unBinResults(queryBinParams, results)
          } else {
            return results
          }
        }
      ],
      options = {
        eliminateNullRows : _eliminateNull,
        renderSpec        : null,
        postProcessors    : postProcessors,
        queryId           : _dimensionIndex
      }

    if (callback) {
      return _cache.queryAsync(query, options, callback);
    } else {
      return _cache.query(query, options);
    }
  }
  /******************************************************************
   * reduce methods
   */
  reduceCount(countExpression, name) {
    this.reduce([{ expression: countExpression, agg_mode: "count", name: name || "val" }])
    return this
  }
  reduceSum(sumExpression, name) {
    this.reduce([{ expression: sumExpression, agg_mode: "sum", name: name || "val" }])
    return this
  }
  reduceAvg(avgExpression, name) {
    this.reduce([{ expression: avgExpression, agg_mode: "avg", name: name || "val" }])
    return this
  }
  reduceMin(minExpression, name) {
    this.reduce([{ expression: minExpression, agg_mode: "min", name: name || "val" }])
    return this
  }
  reduceMax(maxExpression, name) {
    this.reduce([{ expression: maxExpression, agg_mode: "max", name: name || "val" }])
    return this
  }
  // expressions should be an array of
  // { expression, agg_mode (sql_aggregate), name, filter (optional) }
  reduce(expressions) {
    const { _targetSlot } = this,
      dimension                   = this.getDimension(),
      { _dimensionIndex }         = dimension,
      { _targetFilter, _filters } = dimension.getCrossfilter()

    if (!arguments.length) return this._reduceSubExpressions
    this._reduceSubExpressions = expressions
    this._reduceExpression     = ""
    this._reduceVars           = ""

    expressions.forEach((expression, i) => {
      if (i > 0) {
        this._reduceExpression += ","
        this._reduceVars += ","
      }
      if (i === _targetSlot
        && _targetFilter !== null
        && _targetFilter !== _dimensionIndex
        && _filters[_targetFilter] !== "") {

        this._reduceExpression += " AVG(CASE WHEN " + _filters[_targetFilter] + " THEN 1 ELSE 0 END)"
      } else {
        let agg_mode = expression.agg_mode.toUpperCase()
        if (agg_mode === "CUSTOM") {
          this._reduceExpression += expression.expression
        }
        else if (agg_mode === "COUNT") {
          if (expression.filter) {
            this._reduceExpression += "COUNT(CASE WHEN " + expression.filter + " THEN 1 END)"
          } else {
            if (typeof expression.expression !== "undefined") {
              this._reduceExpression += "COUNT(" + expression.expression + ")"
            } else {
              this._reduceExpression += "COUNT(*)"
            }
          }
        } else { // should check for either sum, avg, min, max
          if (expression.filter) {
            this._reduceExpression += agg_mode + "(CASE WHEN " + expression.filter +
              " THEN " +  expression.expression + " END)"
          } else {
            this._reduceExpression += agg_mode + "(" + expression.expression + ")"
          }
        }
      }
      this._reduceExpression += " AS " + expression.name
      this._reduceVars += expression.name
    })
    return this
  }
  size(ignoreFilters, callback) {
    if (!callback) {
      console.warn("Warning: Deprecated sync method group.size(). Please use async version");
    }
    const dataConnector = this._getDataConnector(),
      dimension    = this.getDimension(),
      queryTask    = dataConnector.query.bind(dataConnector),
      sizeAsync    = sizeAsyncWithEffects(queryTask, this.writeFilter),
      sizeSync     = sizeSyncWithEffects(queryTask, this.writeFilter)

    const stateSlice = (({ _isMultiDim, _dimArray } = dimension, {_joinStmt, _tablesStmt } = dimension.getCrossfilter()) => {
      return { _isMultiDim, _joinStmt, _tablesStmt, _dimArray }
    })()

    if (callback) {
      sizeAsync(stateSlice, ignoreFilters, callback)
    } else {
      return sizeSync(stateSlice, ignoreFilters)
    }
  }
  sizeAsync(ignoreFilters) {
    return new Promise((resolve, reject) => {
      this.size(ignoreFilters, (error, data) => {
        if (error) {
          reject(error)
        } else {
          resolve(data)
        }
      })
    })
  }
}
