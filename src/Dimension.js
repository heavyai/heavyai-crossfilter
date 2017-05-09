/**
 * Created by andrelockhart on 5/6/17.
 */
/**
 * A dimension is one or more columns to be queried. This is also used to
 * set 'global' filters on specific columns
 * multidimensional dimension has different behaviors to unidimensional dimension
 */

import { writeQuery } from 'libs/crossfilter/src/sqlWriter/sql-writer'
// todo - is this a singleton?
let instance = null
// todo - this is obviously a god class antipattern, filter is an obvious extraction
export default class Dimension {
    /******************************************************************
     * properties
     */
    type = 'dimension'
    filterVal = null
    _allowTargeted = true
    _selfFilter = null
    _dimensionIndex = isGlobal ? globalFilters.length : filters.length
    _scopedFilters = isGlobal ? globalFilters : filters
    _dimensionGroups = []
    _orderExpression = null
    _scopedFilters.push('')
    _projectExpressions = []
    _projectOnAllDimensionsFlag = false
    _binBounds = null// for binning
    _rangeFilters = []
    _dimContainsArray = []
    _eliminateNull = true
    // option for array columns
    // - means observe own filter and use conjunctive instead of disjunctive between sub-filters
    _drillDownFilter = false
    _cache = resultCache(_dataConnector)
    _dimensionExpression = null
    _samplingRatio = null

    _expression = Array.isArray(expression) ? expression : [expression]

    isMultiDim = expression.length > 1
    _columns = _mapColumnsToNameAndType(crossfilter.getColumns())
    // this the collection of columns, expressed as strings
    // can also cast
    _dimArray = expression.map(function (field) {
        indexOfColumn = _findIndexOfColumn(columns, field)
        isDate = indexOfColumn > -1 && _isDateField(columns[indexOfColumn])
        if (isDate) {
            field = "CAST(" + field + " AS TIMESTAMP(0))"
        }
        return field
    })
    dimensionExpression = _dimArray.includes(null) ? null : _dimArray.join(", ")
    /***********   CONSTRUCTOR   ***************/
    constructor() {
        // singleton enforcer
        if(instance) {
            return instance
        }
        else {
            instance = this
        }
    }
    /******************************************************************
     * private methods
     */

    /******************************************************************
     * public methods
     */
    multiDim (value) {
        if (typeof value === "boolean") {
            isMultiDim = value
            return dimension
        }

        return isMultiDim
    }
    order(orderExpression) {
        _orderExpression = orderExpression;
        return dimension;
    }
    orderNatural() {
        _orderExpression = null;
        return dimension;
    }
    selfFilter(_) {
        if (!arguments.length)
            return _selfFilter;
        _selfFilter = _;
        return dimension;
    }
    allowTargeted(allowTargeted) {
        if (!arguments.length) {
            return _allowTargeted;
        }
        _allowTargeted = allowTargeted;
        return dimension;
    }
    toggleTarget() {
        if (targetFilter == dimensionIndex) { // TODO duplicates isTargeting
            targetFilter = null; // TODO duplicates removeTarget
        } else {
            targetFilter = dimensionIndex;
        }
    }
    removeTarget() {
        if (targetFilter == dimensionIndex) {
            targetFilter = null;
        }
    }
    isTargeting() {
        return targetFilter == dimensionIndex;
    }
    projectOn(expressions) {
        projectExpressions = expressions;
        return dimension;
    }
    projectOnAllDimensions(flag) {
        projectOnAllDimensionsFlag = flag;
        return dimension;
    }
    getFilter() {
        return filterVal;
    }
    getFilterString() {
        return scopedFilters[dimensionIndex];
    }
    filter(range, append = false, resetRange, inverseFilter, binParams = [{extract: false}]) {
        if (typeof range == 'undefined') {
            return filterAll();
        } else if (Array.isArray(range) && !isMultiDim) {
            return filterRange(range, append, resetRange, inverseFilter, binParams);
        } else {
            return filterExact(range, append, inverseFilter, binParams);
        }
    }
    filterRelative(range, append = false, resetRange, inverseFilter) {
        return filterRange(range, append, resetRange, inverseFilter, null, true);
    }
    filterExact(value, append, inverseFilter, binParams = []) {
        value = Array.isArray(value) ? value : [value];
        var subExpression = "";
        for (var e = 0; e < value.length; e++) {
            if (e > 0) {
                subExpression += " AND ";
            }
            var typedValue = formatFilterValue(value[e], true, true);
            if (dimContainsArray[e]) {
                subExpression += typedValue + " = ANY " + dimArray[e];
            } else if (Array.isArray(typedValue)) {
                if (typedValue[0] instanceof Date) {
                    const min = formatFilterValue(typedValue[0]);
                    const max = formatFilterValue(typedValue[1]);
                    const dimension = dimArray[e];
                    subExpression += dimension + " >= " + min + " AND " + dimension + " <= " + max;
                } else {
                    const min = typedValue[0];
                    const max = typedValue[1];
                    const dimension = dimArray[e];
                    subExpression += dimension + " >= " + min + " AND " + dimension + " <= " + max;
                }
            } else {
                if (binParams[e] && binParams[e].extract) {
                    subExpression += "extract(" + binParams[e].timeBin + " from " + uncast(dimArray[e]) +  ") = " + typedValue
                } else {
                    subExpression += typedValue === null ? `${dimArray[e]} IS NULL` : `${dimArray[e]} = ${typedValue}`;
                }
            }
        }
        if (inverseFilter) {
            subExpression = "NOT (" + subExpression + ")";
        }

        if (append) {
            scopedFilters[dimensionIndex] += subExpression;
        } else {
            scopedFilters[dimensionIndex] = subExpression;
        }
        return dimension;
    }
    formNotEqualsExpression(value) {
        var escaped = formatFilterValue(value, true, true);
        return dimensionExpression + " <> " + escaped;
    }
    filterNotEquals(value, append) {
        var escaped = formatFilterValue(value, false, false);
        if (append) {
            scopedFilters[dimensionIndex] += formNotEqualsExpression(value);
        } else {
            scopedFilters[dimensionIndex] = formNotEqualsExpression(value);
        }
        return dimension;
    }
    formLikeExpression(value) {
        var escaped = formatFilterValue(value, false, false);
        return dimensionExpression + " like '%" + escaped + "%'";
    }
    formILikeExpression(value) {
        var escaped = formatFilterValue(value, false, false);
        return dimensionExpression + " ilike '%" + escaped + "%'";
    }
    filterLike(value, append) {
        if (append) {
            scopedFilters[dimensionIndex] += formLikeExpression(value);
        } else {
            scopedFilters[dimensionIndex] = formLikeExpression(value);
        }
        return dimension;
    }
    filterILike(value, append) {
        if (append) {
            scopedFilters[dimensionIndex] += formILikeExpression(value);
        } else {
            scopedFilters[dimensionIndex] = formILikeExpression(value);
        }
        return dimension;
    }
    filterNotLike(value, append) {
        if (append) {
            scopedFilters[dimensionIndex] += "NOT( " + formLikeExpression(value) + ")";
        } else {
            scopedFilters[dimensionIndex] = "NOT( " + formLikeExpression(value) + ")";
        }
        return dimension;
    }
    filterNotILike(value, append) {
        if (append) {
            scopedFilters[dimensionIndex] += "NOT( " + formILikeExpression(value) + ")";
        } else {
            scopedFilters[dimensionIndex] = "NOT( " + formILikeExpression(value) + ")";
        }
        return dimension;
    }
    filterIsNotNull(append) {
        if (append) {
            scopedFilters[dimensionIndex] += `${expression} IS NOT NULL`;
        } else {
            scopedFilters[dimensionIndex] = `${expression} IS NOT NULL`;
        }
        return dimension;
    }
    filterIsNull(append) {
        if (append) {
            scopedFilters[dimensionIndex] += `${expression} IS NULL`;
        } else {
            scopedFilters[dimensionIndex] = `${expression} IS NULL`;
        }
        return dimension;
    }
    filterRange(range, append = false, resetRange, inverseFilters, binParams, isRelative) {
        var isArray = Array.isArray(range[0]); // TODO semi-risky index
        if (!isArray) {
            range = [range];
        }
        filterVal = range;
        var subExpression = "";

        for (var e = 0; e < range.length; e++) {
            if (resetRange === true) {
                rangeFilters[e] = range[e];
            }
            if (e > 0) {
                subExpression += " AND ";
            }

            var typedRange = [
                formatFilterValue(range[e][0], true),
                formatFilterValue(range[e][1], true),
            ];

            if (isRelative) {
                typedRange = [
                    formatRelativeValue(typedRange[0]),
                    formatRelativeValue(typedRange[1])
                ]
            }

            if (binParams && binParams[e] && binParams[e].extract) {
                const dimension = "extract(" + binParams[e].timeBin + " from " + uncast(dimArray[e]) +  ")";

                subExpression += dimension + " >= " + typedRange[0] + " AND " + dimension + " <= " + typedRange[1];
            } else {
                subExpression += dimArray[e] + " >= " + typedRange[0] + " AND " + dimArray[e] + " <= " + typedRange[1];
            }
        }

        if (inverseFilters) {
            subExpression = "NOT(" + subExpression + ")"
        }

        if (append) {
            scopedFilters[dimensionIndex] += "(" + subExpression + ")";
        } else {
            scopedFilters[dimensionIndex] = "(" + subExpression + ")";
        }
        return dimension;
    }
    formatRelativeValue(val) {
        if (val.now) {
            return "NOW()"; // todo - there might be subtle bugs in Now()
        } else if (val.datepart && typeof val.number !== 'undefined') {
            const date = typeof val.date !== 'undefined' ? val.date : "NOW()"
            const operator = typeof val.operator !== 'undefined' ? val.operator : "DATE_ADD"
            const number = isNaN(val.number) ? formatRelativeValue(val.number) : val.number
            const add = typeof val.add !== 'undefined' ? val.add : ""
            return `${operator}(${val.datepart}, ${number}, ${date})${add}`
        } else {
            return val
        }
    }
    filterMulti(filterArray, resetRangeIn, inverseFilters, binParams) {
        var filterWasNull = filters[dimensionIndex] == null || filters[dimensionIndex] == "";
        var resetRange = false;
        if (resetRangeIn !== undefined) {
            resetRange = resetRangeIn;
        }

        var lastFilterIndex = filterArray.length - 1;
        scopedFilters[dimensionIndex] = "(";

        inverseFilters = typeof (inverseFilters) === "undefined" ? false : inverseFilters;

        for (var i = 0; i <= lastFilterIndex; i++) {
            var curFilter = filterArray[i];
            filter(curFilter, true, resetRange, inverseFilters, binParams);
            if (i !== lastFilterIndex) {
                if (drillDownFilter ^ inverseFilters) {
                    filters[dimensionIndex] += " AND ";
                } else {
                    filters[dimensionIndex] += " OR ";
                }
            }
        }
        scopedFilters[dimensionIndex] += ")";
        return dimension;
    }
    filterAll(softFilterClear) {
        if (softFilterClear == undefined || softFilterClear == false) {
            rangeFilters = [];
        }
        filterVal = null;
        scopedFilters[dimensionIndex] = "";
        return dimension;
    }
    samplingRatio(ratio) {
        if (!ratio)
            samplingRatio = null;
        samplingRatio = ratio; // TODO always overwrites; typo?
        return dimension;
    }
    writeTopBottomQuery(k, offset, ascDescExpr, isRender) {
        var query = writeQuery(!!isRender);
        if (!query) {
            return '';
        }

        if (_orderExpression) { // overrides any other ordering based on dimension
            query += " ORDER BY " + _orderExpression + ascDescExpr;
        } else if (dimensionExpression)  {
            query += " ORDER BY " + dimensionExpression + ascDescExpr;
        }

        if (k !== Infinity) {
            query += " LIMIT " + k;
        }
        if (offset !== undefined) {
            query += " OFFSET " + offset;
        }

        return query
    }
}