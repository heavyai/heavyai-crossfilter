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

function notEmpty(item) {
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
function filterNullMeasures(filterStatement, measures) {
    const measureNames          = measures.filter(notEmptyNotStarNotComposite).map(toProp("expression")),
        maybeParseParameters    = flatten(measureNames.map(parseParensIfExist)),
        nullColumnsFilter       = maybeParseParameters.map(isNotNull).join(" AND ")

    return maybeAnd(filterStatement, nullColumnsFilter)
}

function isRelative(sqlStr) { // todo - put all regex in one place (see crossfilter utilities for more regex)
    return /DATE_ADD\(([^,|.]+), (DATEDIFF\(\w+, ?\d+, ?\w+\(\)\)[-+0-9]*|[-0-9]+), ([0-9]+|NOW\(\))\)|NOW\(\)/g.test(sqlStr)
}
function formatFilterValue(value, wrapInQuotes, isExact) {
    const valueType = type(value)
    if (valueType === 'string') {

        let escapedValue = value.replace(/'/g, "''")

        if (!isExact) {
            escapedValue = escapedValue.replace(/%/g, '\\%")
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

// todo - pass in objects vs individual props wherever possible (too many arguments is a code smell)
export function writeFilter(crossfilter, group, dimension, queryBinParams) {
    let filterQuery         = '',
        nonNullFilterCount  = 0,
        allFilters          = crossfilter._filters.concat(crossfilter._globalFilters)

    // we do not observe this dimensions filter
    allFilters.forEach((filter, i) => {
        if ((i !== dimension._dimensionIndex || dimension._drillDownFilter === true)
            && (!dimension._allowTargeted || i !== crossfilter._targetFilter)
            && (filter && filter.length > 0)) {

            // filterQuery != "" is hack as notNullFilterCount was being incremented
            if (nonNullFilterCount > 0 && filterQuery !== "") {
                filterQuery += " AND "
            }
            nonNullFilterCount++
            filterQuery += filter
        }
        else if (i === dimension._dimensionIndex && queryBinParams !== null) {
            let tempBinFilters = '',
                hasBinFilter = false

            if (nonNullFilterCount > 0) {
                tempBinFilters += " AND "
            }
            nonNullFilterCount++

            dimension._dimArray.forEach((dim, i) => {
                // todo - this is malordorous
                if (queryBinParams[i] && typeof queryBinParams[i] !== "undefined" && queryBinParams[i] !== null && !queryBinParams[i].extract) {
                    let queryBounds         = queryBinParams[d].binBounds,
                        tempFilterClause    = ""

                    if (group._boundByFilter === true && dimension._rangeFilters.length > 0) {
                        queryBounds = dimension._rangeFilters[d] // todo - this looks like a potential bug
                    }
                    if (d > 0 && hasBinFilter) {
                        tempBinFilters += " AND "
                    }

                    hasBinFilter = true
                    tempFilterClause += "(" + dim +  " >= " + formatFilterValue(queryBounds[0], true) + " AND " + dim + " <= " + formatFilterValue(queryBounds[1], true) + ")"
                    if (!dimension._eliminateNull) {
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

    if (dimension._selfFilter && filterQuery !== "") {
        filterQuery += " AND " + dimension._selfFilter
    } else if (dimension._selfFilter && filterQuery === "") {
        filterQuery = dimension._selfFilter
    }
    filterQuery = filterNullMeasures(filterQuery, group._reduceSubExpressions)
    return isRelative(filterQuery) ? replaceRelative(filterQuery) : filterQuery
}