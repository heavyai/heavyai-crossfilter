/**
 * Created by andrelockhart on 5/6/17.
 */

/**
 * As opposed to Group, this class is concerned with all columns (e.g. count widget in Immerse)
 */

export default class GroupAll {
    /******************************************************************
     * properties
     */

    /***********   CONSTRUCTOR   ***************/
    constructor(resultCache) {
        // todo - assuming this class is instantiated by another class that holds resultCache, probably CrossFilter?
        this._cache = resultCache
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
        var filterQuery = "";
        var validFilterCount = 0;

        if(!ignoreChartFilters) {

            for (var i = 0; i < filters.length; i++) {
                if (filters[i] && filters[i] != "") {
                    if (validFilterCount > 0) {
                        filterQuery += " AND ";
                    }
                    validFilterCount++;
                    filterQuery += filters[i];
                }
            }
        }

        if(!ignoreFilters) {

            for (var i = 0; i < globalFilters.length; i++) {
                if (globalFilters[i] && globalFilters[i] != "") {
                    if (validFilterCount > 0) {
                        filterQuery += " AND ";
                    }
                    validFilterCount++;
                    filterQuery += globalFilters[i];
                }
            }
        }
        return isRelative(filterQuery) ? replaceRelative(filterQuery) : filterQuery;
    }
    writeQuery(crossfilter, ignoreFilters, ignoreChartFilters) {
        var query = "SELECT " + reduceExpression + " FROM " + _tablesStmt;
        var filterQuery = writeFilter(ignoreFilters, ignoreChartFilters);
        if (filterQuery != "") {
            query += " WHERE " + filterQuery;
        }
        if (crossfilter._joinStmt !== null) {
            if (filterQuery === "") {
                query += " WHERE ";
            } else {
                query += " AND ";
            }
            query += _joinStmt;
        }
        if (_joinStmt !== null) {
            query += " WHERE " + _joinStmt;
        }

        // could use alias "key" here
        return query;
    }
    expressionBuilder(expressionValue, name) {
        // todo - use this to make below methods DRY
    }
    reduceCount(countExpression, name) {
        if (typeof countExpression !== "undefined")
            reduceExpression = "COUNT(" + countExpression + ") as " + (name || "val");
        else
            reduceExpression = "COUNT(*) as val";
        return group;
    }
    reduceSum(sumExpression, name) {
        reduceExpression = "SUM(" + sumExpression + ") as " + (name || "val");
        return group;
    }
    reduceAvg(avgExpression, name) {
        reduceExpression = "AVG(" + avgExpression + ") as " + (name || "val");
        return group;
    }
    reduceMin(minExpression, name) {
        reduceExpression = "MIN(" + minExpression + ") as " + (name || "val");
        return group;
    }
    reduceMax(maxExpression, name) {
        reduceExpression = "MAX(" + maxExpression + ") as " + (name || "val");
        return group;
    }
    reduce(expressions) {
        //expressions should be an array of {expression, agg_mode (sql_aggregate), name}
        reduceExpression = "";
        var numExpressions = expressions.length;
        for (var e = 0; e < numExpressions; e++) {
            if (e > 0) {
                reduceExpression += ",";
            }
            var agg_mode = expressions[e].agg_mode.toUpperCase();

            if (agg_mode === "CUSTOM") {
                reduceExpression += expressions[e].expression;
            } else if (agg_mode == "COUNT") {
                if (typeof expressions[e].expression !== "undefined") {
                    reduceExpression += "COUNT(" + expressions[e].expression + ")";
                } else {

                    reduceExpression += "COUNT(*)";
                }
            } else { // should check for either sum, avg, min, max
                reduceExpression += agg_mode + "(" + expressions[e].expression + ")";
            }
            reduceExpression += " AS " + expressions[e].name;
        }
        return group;
    }
    value(ignoreFilters, ignoreChartFilters, callback) {
        if (!callback) {
            console.warn(
                "Warning: Deprecated sync method groupAll.value(). Please use async version"
            );
        }
        var query = writeQuery(ignoreFilters, ignoreChartFilters);
        var options = {
            eliminateNullRows: false,
            renderSpec: null,
            postProcessors: [function (d) {return d[0].val;}],
            queryId: -1,
        };

        if (callback) {
            return cache.queryAsync(query, options, callback);
        } else {
            return cache.query(query, options);
        }
    }
    valueAsync(ignoreFilters = false, ignoreChartFilters = false) {
        return new Promise((resolve, reject) => {
            value(ignoreFilters, ignoreChartFilters, (error, result) => {
                if (error) {
                    reject(error);
                } else {
                    resolve(result);
                }
            });
        });
    }
    values(ignoreFilters, ignoreChartFilters, callback) {
        if (!callback) {
            console.warn(
                "Warning: Deprecated sync method groupAll.values(). Please use async version"
            );
        }
        var query = writeQuery(ignoreFilters, ignoreChartFilters);
        var options = {
            eliminateNullRows: false,
            renderSpec: null,
            postProcessors: [function (d) {return d[0];}],
            queryId: -1,
        };
        if (callback) {
            return cache.queryAsync(query, options, callback);
        } else {
            return cache.query(query, options);
        }
    }
    valuesAsync(ignoreFilters = false, ignoreChartFilters = false) {
        return new Promise((resolve, reject) => {
            values(ignoreFilters, ignoreChartFilters, (error, data) => {
                if (error) {
                    reject(error);
                } else {
                    resolve(data);
                }
            });
        });
    }
    return reduceCount();

    // todo - end tisws from groupAll() under crossfilter

}