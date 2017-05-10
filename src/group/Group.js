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
    constructor(resultCache) {
        // todo - assuming this class is instantiated by another class that holds resultCache, probably CrossFilter?
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
    writeFilter(crossfilter, queryBinParams) {
        const { _filters, _globalFilters, _targetFilter } = crossfilter
        let filterQuery         = "",
            nonNullFilterCount  = 0,
            allFilters          = _filters.concat(_globalFilters)

        // we do not observe this dimensions filter
        for (var i = 0; i < allFilters.length; i++) {
            if ((i !== dimensionIndex || drillDownFilter === true)
                && (!_allowTargeted || i !== _targetFilter)
                && (allFilters[i] && allFilters[i].length > 0)) {

                // filterQuery != "" is hack as notNullFilterCount was being incremented
                if (nonNullFilterCount > 0 && filterQuery !== "") {
                    filterQuery += " AND "
                }
                nonNullFilterCount++;
                filterQuery += allFilters[i];
            } else if (i === dimensionIndex && queryBinParams !== null) {
                let tempBinFilters = ""

                if (nonNullFilterCount > 0) {
                    tempBinFilters += " AND "
                }

                nonNullFilterCount++
                let hasBinFilter = false

                for (let d = 0; d < dimArray.length; d++) {
                    if (typeof queryBinParams[d] !== "undefined" && queryBinParams[d] !== null && !queryBinParams[d].extract) {
                        let queryBounds = queryBinParams[d].binBounds;
                        let tempFilterClause = ""
                        if (boundByFilter === true && rangeFilters.length > 0) {
                            queryBounds = rangeFilters[d];
                        }

                        if (d > 0 && hasBinFilter) {
                            tempBinFilters += " AND ";
                        }

                        hasBinFilter = true;
                        tempFilterClause += "(" + dimArray[d] +  " >= " + formatFilterValue(queryBounds[0], true) + " AND " + dimArray[d] + " <= " + formatFilterValue(queryBounds[1], true) + ")";
                        if (!eliminateNull) {
                            tempFilterClause = `(${tempFilterClause} OR (${dimArray[d]} IS NULL))`
                        }
                        tempBinFilters += tempFilterClause
                    }
                }

                if (hasBinFilter) {
                    filterQuery += tempBinFilters
                }
            }
        }


        if (_selfFilter && filterQuery !== "") {
            filterQuery += " AND " + _selfFilter;
        } else if (_selfFilter && filterQuery == "") {
            filterQuery = _selfFilter;
        }
        filterQuery = filterNullMeasures(filterQuery, reduceSubExpressions);
        return isRelative(filterQuery) ? replaceRelative(filterQuery) : filterQuery;
    }
    getBinnedDimExpression(expression, binBounds, numBins, timeBin, extract) { // jscs:ignore maximumLineLength
        var isDate = type(binBounds[0]) == "date";
        numBins = numBins || 0;
        if (isDate) {
            if (timeBin) {
                if (!!extract) {
                    return "extract(" + timeBin + " from " + uncast(expression) + ")";
                } else {
                    return "date_trunc(" + timeBin + ", " + expression + ")";
                }
            } else {
                // TODO(croot): throw error if no num bins?
                var dimExpr = "extract(epoch from " + expression + ")";

                // as javscript epoch is in ms
                var filterRange = (binBounds[1].getTime() - binBounds[0].getTime()) * 0.001;

                var binsPerUnit = (numBins / filterRange);
                var lowerBoundsUTC = binBounds[0].getTime() / 1000;
                const binnedExpression = "cast(" +
                    "(" + dimExpr + " - " + lowerBoundsUTC + ") * " + binsPerUnit + " as int)";
                return binnedExpression;
            }
        } else {
            // TODO(croot): throw error if no num bins?
            var filterRange = binBounds[1] - binBounds[0];

            var binsPerUnit = (numBins / filterRange);

            if (filterRange === 0) {
                binsPerUnit = 0
            }
            const binnedExpression = "cast(" +
                "(cast(" + expression + " as float) - " + binBounds[0] + ") * " + binsPerUnit + " as int)";
            return binnedExpression;
        }
    }
    // todo - use/rationalize sql-writer.writeQuery
    writeQuery(queryBinParams, sortByValue, ignoreFilters, hasRenderSpec) {
        var query = null;
        if (reduceSubExpressions
            && (_allowTargeted && (targetFilter !== null || targetFilter !== lastTargetFilter))) {
            reduce(reduceSubExpressions);
            lastTargetFilter = targetFilter;
        }

        //var tableSet = {};
        // first clone _reduceTableSet
        //for (key in _reduceTableSet)
        //  tableSet[key] = _reduceTableSet[key];
        query = "SELECT ";
        var projectExpressions = this.buildProjectExpressions(hasRenderSpec, queryBinParams);
        if (!projectExpressions) {
            return "";
        }
        query += projectExpressions.join(',');

        query += checkForSortByAllRows() + " FROM " + _tablesStmt;

        function checkForSortByAllRows() {
            // TODO(croot): this could be used as a driver for some kind of
            // scale when rendering, so it should be exposed a better way
            // and returned when getProjectOn() is called.
            return sortByValue === "countval" ? ", COUNT(*) AS countval" : "";
        }

        var filterQuery = ignoreFilters ? "" : writeFilter(queryBinParams);
        if (filterQuery !== "") {
            query += " WHERE " + filterQuery;

        }
        if (_joinStmt !== null) {
            if (filterQuery === "") {
                query += " WHERE ";
            } else {
                query += " AND ";
            }
            query += _joinStmt;
        }

        // could use alias "key" here
        query += " GROUP BY ";
        for (var i = 0; i < dimArray.length; i++) {
            if (i !== 0) {
                query += ", ";
            }

            if (!!hasRenderSpec && dimArray[i].match(/rowid\s*$/)) {
                // do not cast rowid with 'as key[0-9]'
                // as that will mess up hit-test renders
                // and poly renders.
                query += dimArray[i];
            } else {
                query += "key" + i.toString();
            }
        }

        if (queryBinParams !== null) {
            var havingClause = " HAVING ";
            var hasBinParams = false;
            for (var d = 0; d < queryBinParams.length; d++) {
                if (queryBinParams[d] !== null && !queryBinParams[d].timeBin) {
                    let havingSubClause = ""
                    if (d > 0 && hasBinParams) {
                        havingClause += " AND ";
                    }
                    hasBinParams = true;
                    havingSubClause += "key" + d.toString() + " >= 0 AND key" +
                        d.toString() + " < " + queryBinParams[d].numBins;
                    if (!eliminateNull) {
                        havingSubClause = `(${havingSubClause} OR key${d.toString()} IS NULL)`
                    }
                    havingClause += havingSubClause
                }
            }
            if (hasBinParams) {
                query += havingClause;
            }
        }

        return query;
    }
    setBoundByFilter(boundByFilterIn) {
        boundByFilter = boundByFilterIn;
        return group;
    }
    order(orderExpression) {
        _orderExpression = orderExpression;
        return group;
    }
    orderNatural() {
        _orderExpression = null;
        return group;
    }
    all(callback) {
        if (!callback) {
            console.warn("Warning: Deprecated sync method group.all(). Please use async version");
        }

        // freeze bin params so they don't change out from under us
        var queryBinParams = binParams();
        if (!queryBinParams.length) {
            queryBinParams = null;
        }
        var query = writeQuery(queryBinParams);
        query += " ORDER BY ";
        for (var d = 0; d < dimArray.length; d++) {
            if (d > 0)
                query += ",";
            query += "key" + d.toString();
        }

        var postProcessors = [
            function unBinResultsForAll(results) {
                if (queryBinParams) {
                    const filledResults = fillBins(this, queryBinParams, results);
                    return unBinResults(queryBinParams, filledResults);
                } else {
                    return results
                }
            }
        ]

        var options = {
            eliminateNullRows: eliminateNull,
            renderSpec: null,
            postProcessors: postProcessors,
            queryId: dimensionIndex,
        };

        if (callback) {
            return cache.queryAsync(query, options, callback);
        } else {
            return cache.query(query, options);
        }
    }
    minMaxWithFilters({min = "min_val", max = "max_val"} = {}) {
        const filters = writeFilter();
        const filterQ = filters.length ? `WHERE ${filters}` : ""
        const query = `SELECT MIN(${dimArray[0]}) as ${min}, MAX(${dimArray[0]}) as ${max} FROM ${_tablesStmt} ${filterQ}`;

        var options = {
            eliminateNullRows: eliminateNull,
            postProcessors: [(d) => d[0]],
            renderSpec: null,
            queryId: -1,
        };

        return new Promise((resolve, reject) => {
            cache.queryAsync(query, options, (error, val) => {
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
        var queryBinParams = binParams();
        var query = writeQuery((queryBinParams.length ? queryBinParams : null), _orderExpression, ignoreFilters, !!isRender);

        if (!query) {
            return '';
        }

        query += " ORDER BY ";
        if (_orderExpression) {
            query += _orderExpression + ascDescExpr;
        } else {
            var reduceArray = reduceVars.split(",");
            var reduceSize = reduceArray.length;
            for (var r = 0; r < reduceSize - 1; r++) {
                query += reduceArray[r] + ascDescExpr + ",";
            }
            query += reduceArray[reduceSize - 1] + ascDescExpr;
        }

        if (k != Infinity) {
            query += " LIMIT " + k;
        }
        if (offset !== undefined)
            query += " OFFSET " + offset;

        return query;
    }
    // todo - see sql-writer
    writeTopQuery(k, offset, ignoreFilters, isRender) {
        return writeTopBottomQuery(k, offset, " DESC", ignoreFilters, isRender);
    }
    // todo - see sql-writer
    top(k, offset, renderSpec, callback, ignoreFilters) {
        if (!callback) {
            console.warn("Warning: Deprecated sync method group.top(). Please use async version");
        }

        // freeze bin params so they don't change out from under us
        var queryBinParams = binParams();
        if (!queryBinParams.length) {
            queryBinParams = null;
        }

        var query = writeTopQuery(k, offset, ignoreFilters, !!renderSpec);

        var postProcessors = [
            function unBinResultsForTop(results) {
                if (queryBinParams) {
                    return unBinResults(queryBinParams, results);
                } else {
                    return results
                }
            },
        ];

        var options = {
            eliminateNullRows: eliminateNull,
            renderSpec: renderSpec,
            postProcessors: postProcessors,
            queryId: dimensionIndex,
        };

        if (callback) {
            return cache.queryAsync(query, options, callback);
        } else {
            return cache.query(query, options);
        }
    }
    // todo - see sql-writer
    topAsync (k, offset, renderSpec, ignoreFilters) {
        return new Promise((resolve, reject) => {
            top(k, offset, renderSpec, (error, result) => {
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
        return writeTopBottomQuery(k, offset, "", ignoreFilters, isRender);
    }
    // todo - see sql-writer
    bottom(k, offset, renderSpec, callback, ignoreFilters) {
        if (!callback) {
            console.warn(
                "Warning: Deprecated sync method group.bottom(). Please use async version"
            );
        }

        // freeze bin params so they don't change out from under us
        var queryBinParams = binParams();
        if (!queryBinParams.length) {
            queryBinParams = null;
        }

        var query = writeBottomQuery(k, offset, ignoreFilters, !!renderSpec);

        var postProcessors = [
            function unBinResultsForBottom(results) {
                if (queryBinParams) {
                    return unBinResults(queryBinParams, results);
                } else {
                    return results;
                }
            },
        ];

        var options = {
            eliminateNullRows: eliminateNull,
            renderSpec: null,
            postProcessors: postProcessors,
            queryId: dimensionIndex,
        };

        if (callback) {
            return cache.queryAsync(query, options, callback);
        } else {
            return cache.query(query, options);
        }
    }
    reduceCount(countExpression, name) {
        reduce([{ expression: countExpression, agg_mode: "count", name: name || "val" }]);
        return group;
    }
    reduceSum(sumExpression, name) {
        reduce([{ expression: sumExpression, agg_mode: "sum", name: name || "val" }]);
        return group;
    }
    reduceAvg(avgExpression, name) {
        reduce([{ expression: avgExpression, agg_mode: "avg", name: name || "val" }]);
        return group;
    }
    reduceMin(minExpression, name) {
        reduce([{ expression: minExpression, agg_mode: "min", name: name || "val" }]);
        return group;
    }
    reduceMax(maxExpression, name) {
        reduce([{ expression: maxExpression, agg_mode: "max", name: name || "val" }]);
        return group;
    }
    // expressions should be an array of
    // { expression, agg_mode (sql_aggregate), name, filter (optional) }
    reduce(crossfilter, dimension, expressions) {
        // _reduceTableSet = {};
        let { _reduceExpression, _reduceSubExpressions, _reduceVars, _targetSlot } = this

        if (!arguments.length) {
            return _reduceSubExpressions
        }
        _reduceSubExpressions = expressions
        _reduceExpression     = ""
        _reduceVars           = ""

        expressions.forEach((expression, index) => {
            if (index > 0) {
                _reduceExpression += ","
                _reduceVars += ","
            }
            if (index === _targetSlot
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
            sizeAsync    = sizeAsyncWithEffects(queryTask, writeFilter),
            sizeSync     = sizeSyncWithEffects(queryTask, writeFilter)
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

    // return reduceCount();
}