/**
 * Created by andrelockhart on 5/6/17.
 */
/**
 * Group: A child of the dimension that receives state/context from the dimension
 * so it knows what table it's in, what filters it's using, etc. so it can write a query
 * state that group is concerned with is aggregated
 * It also is concerned with how data is binned, temporal binning is treated
 * differently than quantitative binning
 * For example, in Immerse, Measures are used as part of the grouping query (aggregate)
 *  THIS IS WHAT THE CHARTING LIBRARY USES TO WRITE QUERIES AND MAKE REQUESTS
 */
import fillBins from './Bins'
import {sizeAsyncWithEffects, sizeSyncWithEffects} from "./group-utilities"
import { formatFilterValue, replaceRelative } from '../Filter'

export default class Group {
    /******************************************************************
     * properties
     */
    type = 'group' // todo - tisws
    _reduceExpression = null  // count will become default
    _reduceSubExpressions = null
    _reduceVars = null
    _boundByFilter = false
    _dateTruncLevel = null
    _lastTargetFilter = null
    _targetSlot = 0
    timeParams = null
    _fillMissingBins = false
    _orderExpression = null
    _reduceTableSet = {}
    _binParams = []
    // dimensionGroups.push(group) //  todo - tisws
    /** getters/setters **/
    get binParams() {
        return this._binParams
    }
    set binParams(binParams) {
        if(binParams) this._binParams = binParams
    }
    // binParams(binParamsIn) {
    //     if (!arguments.length) {
    //         return this._binParams
    //     }
    //
    //     this._binParams = binParamsIn
    //     return this // todo - tisws
    // }
    /***********   CONSTRUCTOR   ***************/
    constructor(crossfilter, dimension, resultCache) {
        // todo - assuming this class is instantiated by another class that holds resultCache, probably CrossFilter?
        this.init(crossfilter, dimension, resultCache)
    }
    /***********   INITIALIZATION   ***************/
    init(crossfilter, dimension, resultCache) {
        this._cache = resultCache
    }
    /******************************************************************
     * private methods
     */

    /******************************************************************
     * public methods
     */
    buildProjectExpressions(dimension, isRenderQuery, queryBinParams) {
        let { _boundByFilter, _binParams, _reduceExpression }  = this,
            { _dimArray, _rangeFilters, _dimContainsArray }    = dimension,
            projectExpressions                                 = []

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
                projectExpressions.push(binnedExpression + " as key" + d.toString());
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
                );
                projectExpressions.push(binnedExpression + " as key" + d.toString())
            }
            else {
                if (!!isRenderQuery && dimArray[d].match(/rowid\s*$/)) {
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
    writeFilter(crossfilter, dimension, queryBinParams) {
        const { _filters, _globalFilters, _targetFilter } = crossfilter,
            { _selfFilter, _allowTargeted, _dimensionIndex, _drillDownFilter, _dimArray, _eliminateNull, _rangeFilters } = dimension
        let filterQuery         = "",
            nonNullFilterCount  = 0,
            allFilters          = _filters.concat(_globalFilters)

        // we do not observe this dimensions filter
        allFilters.forEach((allFilter, i) => {
            if ((i !== _dimensionIndex || _drillDownFilter === true)
                && (!_allowTargeted || i !== _targetFilter)
                && (allFilter && allFilter.length > 0)) {

                // filterQuery != "" is hack as notNullFilterCount was being incremented
                if (nonNullFilterCount > 0 && filterQuery !== "") {
                    filterQuery += " AND "
                }
                nonNullFilterCount++
                filterQuery += allFilter
            }
            else if (i === _dimensionIndex && queryBinParams !== null) {
                let tempBinFilters = ""

                if (nonNullFilterCount > 0) {
                    tempBinFilters += " AND "
                }
                nonNullFilterCount++
                let hasBinFilter = false

                for (let d = 0; d < _dimArray.length; d++) {

                    if (typeof queryBinParams[d] !== "undefined" && queryBinParams[d] !== null && !queryBinParams[d].extract) {
                        let queryBounds      = queryBinParams[d].binBounds,
                            tempFilterClause = ""
                        if (this._boundByFilter === true && _rangeFilters.length > 0) {
                            queryBounds = _rangeFilters[d]
                        }
                        if (d > 0 && hasBinFilter) {
                            tempBinFilters += " AND "
                        }

                        hasBinFilter = true
                        tempFilterClause += "(" + _dimArray[d] +  " >= " + formatFilterValue(queryBounds[0], true) + " AND " + _dimArray[d] + " <= " + formatFilterValue(queryBounds[1], true) + ")"
                        if (!_eliminateNull) {
                            tempFilterClause = `(${tempFilterClause} OR (${_dimArray[d]} IS NULL))`
                        }
                        tempBinFilters += tempFilterClause
                    }
                }
                if (hasBinFilter) {
                    filterQuery += tempBinFilters
                }
            }
        })
        if (_selfFilter && filterQuery !== "") {
            filterQuery += " AND " + _selfFilter
        }
        else if (_selfFilter && filterQuery === "") {
            filterQuery = _selfFilter
        }
        filterQuery = filterNullMeasures(filterQuery, this._reduceSubExpressions)
        return isRelative(filterQuery) ? replaceRelative(filterQuery) : filterQuery
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
                // TODO(croot): throw error if no num bins?
                const dimExpr = "extract(epoch from " + expression + ")",
                    // as javscript epoch is in ms
                    filterRange     = (binBounds[1].getTime() - binBounds[0].getTime()) * 0.001, // todo - is .001 always safe?
                    binsPerUnit     = (numBins / filterRange),
                    lowerBoundsUTC  = binBounds[0].getTime() / 1000
                return "cast(" +
                    "(" + dimExpr + " - " + lowerBoundsUTC + ") * " + binsPerUnit + " as int)"
            }
        } else {
            // TODO(croot): throw error if no num bins?
            let filterRange = binBounds[1] - binBounds[0],
                binsPerUnit = (numBins / filterRange)

            if (filterRange === 0) {
                binsPerUnit = 0
            }
            return "cast(" +
                "(cast(" + expression + " as float) - " + binBounds[0] + ") * " + binsPerUnit + " as int)"
        }
    }
    /** query writing methods **/
    // todo - use/rationalize sql-writer.writeQuery
    writeQuery(crossfilter, dimension, queryBinParams, sortByValue, ignoreFilters, hasRenderSpec) {
        let query = "SELECT "
        const { _targetFilter, _tablesStmt, _joinStmt } = crossfilter,
            { _allowTargeted, _dimArray, _eliminateNull } = dimension
        if (this._reduceSubExpressions
            && (_allowTargeted && (_targetFilter !== null || _targetFilter !== this._lastTargetFilter))) {
            this.reduce(this._reduceSubExpressions)
            this._lastTargetFilter = _targetFilter
        }
        let projectExpressions = this.buildProjectExpressions(hasRenderSpec, queryBinParams)
        if (!projectExpressions) {
            return ""
        }
        query += projectExpressions.join(',')
        query += checkForSortByAllRows() + " FROM " + _tablesStmt

        function checkForSortByAllRows() {
            // TODO(croot): this could be used as a driver for some kind of
            // scale when rendering, so it should be exposed a better way
            // and returned when buildProjectExpressions() is called.
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
                hasBinParams = false

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
    all(dimension, callback) {
        const { _dimArray, _eliminateNull, _dimensionIndex, _cache } = dimension
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
        for (let d = 0; d < _dimArray.length; d++) {
            if (d > 0)
                query += ","
            query += "key" + d.toString()
        }

        const postProcessors = [
            function unBinResultsForAll(results) {
                if (queryBinParams) {
                    const filledResults = fillBins(this, queryBinParams, results)
                    return this.unBinResults(queryBinParams, filledResults)
                } else {
                    return results
                }
            }
        ]
        const options = {
            eliminateNullRows: _eliminateNull,
            renderSpec: null,
            postProcessors: postProcessors,
            queryId: _dimensionIndex
        }
        if (callback) {
            return _cache.queryAsync(query, options, callback)
        } else {
            return _cache.query(query, options)
        }
    }
    minMaxWithFilters(crossfilter, {min = "min_val", max = "max_val"} = {}) {
        const { _dimArray, _eliminateNull, _cache } = dimension,
            filters = this.writeFilter(),
            filterQ = filters.length ? `WHERE ${filters}` : "",
            query   = `SELECT MIN(${_dimArray[0]}) as ${min}, MAX(${_dimArray[0]}) as ${max} FROM ${crossfilter._tablesStmt} ${filterQ}`,

            options = {
            eliminateNullRows: _eliminateNull,
            postProcessors: [(d) => d[0]],
            renderSpec: null,
            queryId: -1
        }

        return new Promise((resolve, reject) => {
            _cache.queryAsync(query, options, (error, val) => {
                if (error) {
                    reject(error)
                } else {
                    resolve(val)
                }
            })
        })
    }
    // todo - see sql-writer
    writeTopBottomQuery(k, offset, ascDescExpr, ignoreFilters, isRender) {
        const { _orderExpression, _reduceVars } = this,
            queryBinParams                      = this.binParams()
        let query = this.writeQuery((queryBinParams.length ? queryBinParams : null), _orderExpression, ignoreFilters, !!isRender)

        if (!query) {
            return ''
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
    // todo - see sql-writer
    writeTopQuery(k, offset, ignoreFilters, isRender) {
        return this.writeTopBottomQuery(k, offset, " DESC", ignoreFilters, isRender)
    }
    // todo - see sql-writer
    top(dimension, k, offset, renderSpec, callback, ignoreFilters) {
        const { _eliminateNull, _dimensionIndex, _cache } = dimension
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
                    if (queryBinParams) { // todo - scope?
                        return this.unBinResults(queryBinParams, results)
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
    // todo - see sql-writer
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
    // todo - see sql-writer
    writeBottomQuery(k, offset, ignoreFilters, isRender) {
        return this.writeTopBottomQuery(k, offset, "", ignoreFilters, isRender)
    }
    // todo - see sql-writer
    bottom(dimension, k, offset, renderSpec, callback, ignoreFilters) {
        const { _eliminateNull, _dimensionIndex, _cache } = dimension
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

        let query = this.writeBottomQuery(k, offset, ignoreFilters, !!renderSpec),
            postProcessors = [
                function unBinResultsForBottom(results) {
                    if (queryBinParams) {
                        return this.unBinResults(queryBinParams, results)
                    } else {
                        return results
                    }
                }
            ],
            options = {
                eliminateNullRows: _eliminateNull,
                renderSpec: null,
                postProcessors: postProcessors,
                queryId: _dimensionIndex
            }

        if (callback) {
            return _cache.queryAsync(query, options, callback);
        } else {
            return _cache.query(query, options);
        }
    }
    /** reduce methods **/
    reduceCount(countExpression, name) {
        reduce([{ expression: countExpression, agg_mode: "count", name: name || "val" }])
        return this
    }
    reduceSum(sumExpression, name) {
        reduce([{ expression: sumExpression, agg_mode: "sum", name: name || "val" }])
        return this
    }
    reduceAvg(avgExpression, name) {
        reduce([{ expression: avgExpression, agg_mode: "avg", name: name || "val" }])
        return this
    }
    reduceMin(minExpression, name) {
        reduce([{ expression: minExpression, agg_mode: "min", name: name || "val" }])
        return this
    }
    reduceMax(maxExpression, name) {
        reduce([{ expression: maxExpression, agg_mode: "max", name: name || "val" }])
        return this
    }
    // expressions should be an array of
    // { expression, agg_mode (sql_aggregate), name, filter (optional) }
    reduce(crossfilter, dimension, expressions) {
        let { _reduceExpression, _reduceSubExpressions, _reduceVars, _targetSlot } = this

        if (!arguments.length) {
            return _reduceSubExpressions
        }
        _reduceSubExpressions = expressions
        _reduceExpression     = ""
        _reduceVars           = ""

        expressions.forEach((expression, i) => {
            if (i > 0) {
                _reduceExpression += ","
                _reduceVars += ","
            }
            if (i === _targetSlot
                && crossfilter._targetFilter !== null
                && crossfilter._targetFilter !== dimension._dimensionIndex
                && crossfilter._filters[crossfilter._targetFilter] !== "") {

                _reduceExpression += " AVG(CASE WHEN " + crossfilter._filters[crossfilter._targetFilter] + " THEN 1 ELSE 0 END)"
            } else {
                /*
                 * if (expressions[e].expression in columnTypeMap) {
                 *   _reduceTableSet[columnTypeMap[expressions[e].expression].table] =
                 *     (_reduceTableSet[columnTypeMap[expressions[e].expression].table] || 0) + 1;
                 *  }
                 */
                let agg_mode = expression.agg_mode.toUpperCase()

                if (agg_mode === "CUSTOM") {
                    _reduceExpression += expression.expression
                }
                else if (agg_mode === "COUNT") {
                    if (expression.filter) {
                        _reduceExpression += "COUNT(CASE WHEN " + expression.filter + " THEN 1 END)";
                    } else {
                        if (typeof expression.expression !== "undefined") {
                            _reduceExpression += "COUNT(" + expression.expression + ")"
                        } else {
                            _reduceExpression += "COUNT(*)"
                        }
                    }
                } else { // should check for either sum, avg, min, max
                    if (expression.filter) {
                        _reduceExpression += agg_mode + "(CASE WHEN " + expression.filter +
                            " THEN " +  expression.expression + " END)"
                    } else {
                        _reduceExpression += agg_mode + "(" + expression.expression + ")"
                    }
                }
            }
            _reduceExpression += " AS " + expression.name
            _reduceVars += expression.name
        })
        return this
    }
    size(ignoreFilters, callback) {
        if (!callback) {
            console.warn("Warning: Deprecated sync method group.size(). Please use async version");
        }
        const stateSlice = { isMultiDim, _joinStmt, _tablesStmt, dimArray },
            queryTask    = _dataConnector.query.bind(_dataConnector),
            sizeAsync    = sizeAsyncWithEffects(queryTask, this.writeFilter),
            sizeSync     = sizeSyncWithEffects(queryTask, this.writeFilter)
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
    // return reduceCount(); // todo
}