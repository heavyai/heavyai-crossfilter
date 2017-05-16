/**
 * Created by andrelockhart on 5/6/17.
 */
import moment from 'moment'

const TYPES = {
    'undefined': 'undefined',
    'number': 'number',
    'boolean': 'boolean',
    'string': 'string',
    '[object Function]': 'function',
    '[object RegExp]': 'regexp',
    '[object Array]': 'array',
    '[object Date]': 'date',
    '[object Error]': 'error'
}

const TOSTRING = Object.prototype.toString

function type(o) {
    return TYPES[typeof o] || TYPES[TOSTRING.call(o)] || (o ? 'object' : 'null')
}

function notEmptyNotStarNotComposite(item) {
    return notEmpty(item.expression) && item.expression !== "*" && !item.isComposite
}
function toProp(propName) { return item => item[propName] }
function flatten(arr) {
    return arr.reduce((flat, toFlatten) => {
        return flat.concat(Array.isArray(toFlatten) ? flatten(toFlatten) : toFlatten)
    }, [])
}
function maybeAnd(clause1, clause2) {
    const joiningWord = clause1 === "" || clause2 === "" ? "" : " AND "
    return clause1 + joiningWord + clause2
}
function isNotNull(columnName) { return columnName + " IS NOT NULL" }

export function isRelative(sqlStr) { // todo - put all regex in one place (see crossfilter utilities for more regex)
    return /DATE_ADD\(([^,|.]+), (DATEDIFF\(\w+, ?\d+, ?\w+\(\)\)[-+0-9]*|[-0-9]+), ([0-9]+|NOW\(\))\)|NOW\(\)/g.test(sqlStr)
}
export function notEmpty(item) {
    switch (typeof item) {
        case 'undefined': return false
        case 'boolean'  : return true
        case 'number'   : return true
        case 'symbol'   : return true
        case 'function' : return true
        case 'string'   : return item.length > 0
        // null, array, object, date
        // todo: tisws -- item.getDay
        case 'object'   : return item !== null && (typeof item.getDay === 'function' || Object.keys(item).length > 0) // jscs:ignore maximumLineLength
    }
}
export function parseParensIfExist(measureValue) {
    // slightly hacky regex, but goes down for 4 levels deep in terms of nesting ().
    const checkParens  = /\(([^()]*|\(([^()]*|\(([^()]*|\([^()]*\))*\))*\))*\)/g,
        thereAreParens = checkParens.test(measureValue)

    if (thereAreParens) {
        const parsedParens = measureValue.match(checkParens)
        return parsedParens.map((str) => {
            return str.slice(1, -1)
        })
    } else {
        return [measureValue]
    }
}
export function filterNullMeasures(filterStatement, measures) {
    const measureNames          = measures.filter(notEmptyNotStarNotComposite).map(toProp("expression")),
        maybeParseParameters    = flatten(measureNames.map(parseParensIfExist)),
        nullColumnsFilter       = maybeParseParameters.map(isNotNull).join(" AND ")

    return maybeAnd(filterStatement, nullColumnsFilter)
}
export function formatFilterValue(value, wrapInQuotes, isExact) {
    const valueType = type(value)
    if (valueType === 'string') {

        let escapedValue = value.replace(/'/g, "''")

        if (!isExact) {
            escapedValue = escapedValue.replace(/%/g, "\\%")
            escapedValue = escapedValue.replace(/_/g, "\\_")
        }

        return wrapInQuotes ? "'" + escapedValue + "'" : escapedValue;
    } else if (valueType === 'date') {
        return "TIMESTAMP(0) '" + value.toISOString().slice(0, 19).replace("T", " ") + "'"
    } else {
        return value
    }
}
export function replaceRelative(sqlStr) { // todo - put all regex in one place (see crossfilter utilities for more regex)
    const relativeDateRegex = /DATE_ADD\(([^,|.]+), (DATEDIFF\(\w+, ?\d+, ?\w+\(\)\)[-+0-9]*|[-0-9]+), ([0-9]+|NOW\(\))\)/g,
          withRelative      = sqlStr.replace(relativeDateRegex, (match,datepart,number,date) => {
            if (isNaN(number)) {
                const num = Number(number.slice(number.lastIndexOf(")")+1))
                if (isNaN(num)) {
                    return formatFilterValue(moment().utc().startOf(datepart).toDate(), true)
                } else {
                    return formatFilterValue(moment().add(num, datepart).utc().startOf(datepart).toDate(), true)
                }
            } else {
                return formatFilterValue(moment().add(number, datepart).toDate(), true)
            }
        })
    return withRelative.replace(/NOW\(\)/g, formatFilterValue(moment().toDate(), true))
}
export function writeGroupFilter(queryBinParams, group) {
    const dimension                                 = group.getDimension(),
        { _boundByFilter, _reduceSubExpressions }   = group,
        { _filters, _globalFilters, _targetFilter } = dimension.getCrossfilter(),
        { _dimensionIndex, _drillDownFilter, _allowTargeted, _dimArray, _rangeFilters, _eliminateNull, _selfFilter } = dimension
    let filterQuery         = '',
        nonNullFilterCount  = 0,
        allFilters          = _filters.concat(_globalFilters)

    // we do not observe this dimensions filter
    allFilters.forEach((filter, i) => {
        if ((i !== _dimensionIndex || _drillDownFilter === true)
            && (!_allowTargeted || i !== _targetFilter)
            && (filter && filter.length > 0)) {

            // filterQuery != "" is hack as notNullFilterCount was being incremented
            if (nonNullFilterCount > 0 && filterQuery !== "") {
                filterQuery += " AND "
            }
            nonNullFilterCount++
            filterQuery += filter
        }
        else if (i === _dimensionIndex && queryBinParams !== null) {
            let tempBinFilters = '',
                hasBinFilter = false

            if (nonNullFilterCount > 0) {
                tempBinFilters += " AND "
            }
            nonNullFilterCount++

            _dimArray.forEach((dim, i) => {
                // todo - this is malordorous
                if (queryBinParams[i] && typeof queryBinParams[i] !== "undefined" && queryBinParams[i] !== null && !queryBinParams[i].extract) {
                    let queryBounds         = queryBinParams[d].binBounds,
                        tempFilterClause    = ""

                    if (_boundByFilter === true && _rangeFilters.length > 0) {
                        queryBounds = _rangeFilters[d] // todo - this looks like a potential bug
                    }
                    if (d > 0 && hasBinFilter) {
                        tempBinFilters += " AND "
                    }

                    hasBinFilter = true
                    tempFilterClause += "(" + dim +  " >= " + formatFilterValue(queryBounds[0], true) + " AND " + dim + " <= " + formatFilterValue(queryBounds[1], true) + ")"
                    if (!_eliminateNull) {
                        tempFilterClause = `(${tempFilterClause} OR (${dim} IS NULL))`
                    }
                    tempBinFilters += tempFilterClause
                }
            })
            if (hasBinFilter) {
                filterQuery += tempBinFilters
            }
        }
    })

    if (_selfFilter && filterQuery !== "") {
        filterQuery += " AND " + _selfFilter
    } else if (_selfFilter && filterQuery === "") {
        filterQuery = _selfFilter
    }
    filterQuery = filterNullMeasures(filterQuery, _reduceSubExpressions)
    return isRelative(filterQuery) ? replaceRelative(filterQuery) : filterQuery
}