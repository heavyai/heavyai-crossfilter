/**
 * Created by andrelockhart on 5/5/17.
 */
import {isRelative, replaceRelative} from 'utils/CrossFilterUtilities'

// todo - make these sharable thruout app
const SELECT    = "SELECT ",
      FROM      = " FROM ",
      WHERE     = " WHERE ",
      AND       = " AND ",
      ORDER_BY  = " ORDER BY ",
      ASC       = " ASC",
      DESC      = " DESC",
      LIMIT     = " LIMIT ",
      OFFSET    = " OFFSET "

// todo - there is a metric ton of SQL writing fragments that should be organized here

/************   sql fragments from other files/classes   *************************




 */

// https://lowrey.me/exploring-knuths-multiplicative-hash-2/
const knuthHash              = '265445761' // knuthMultiplicativeHash
const distributionBitLimit32 = '4294967296'

function convertDimensionArraysToString(crossfilter, dimension, hasRenderSpec, rowIdAttr) {
    let projList = ''
    if (dimension._projectOnAllDimensionsFlag) {
        const dimensions        = crossfilter.getDimensions()
        let nonNullDimensions   = [],
            dimSet              = {}

        // todo - make function to do forEach so DRY (seems to un-sparse array?)
        dimensions.forEach((dimensionItem) => {
            // other conditions:
            // && dimensions[d] in columnTypeMap && !columnTypeMap[dimensions[d]].is_array
            if(dimensionItem !== null && dimensionItem !== '') {
                nonNullDimensions.push(dimensionItem)
            }
        })

        nonNullDimensions = nonNullDimensions.concat(dimension.projectExpressions)
        // now make set of unique non null dimensions
        nonNullDimensions.forEach((nonNullDimension) => {
            if (!(nonNullDimension in dimSet)) {
                dimSet[nonNullDimension] = null
            }
        })

        nonNullDimensions = []
        dimSet.ownKeys.map((key) => {
            nonNullDimensions.push(key)
        })

        projList = nonNullDimensions.join(",");
    } else {
        projList = dimension.projectExpressions.join(",");
    }

    if (hasRenderSpec) {
        if (projList.indexOf('rowid') < 0 && projList.indexOf(rowIdAttr) < 0) {
            projList += "," + rowIdAttr
        }
    }
    return projList === '' ? false : projList
}

// Returns the top K selected records based on this dimension"s order.
// Note: observes this dimension"s filter, unlike group and groupAll.
function writeQuery(crossfilter, dimension, hasRenderSpec, dataTables) {
    // todo - dataTables[0] looks brittle
    const rowIdAttr = dataTables[0] + '.rowid'
    let projList = convertDimensionArraysToString(crossfilter, hasRenderSpec, rowIdAttr) // todo - rename projList to something semantic
    // stops query from happening if variables do not exist in chart
    if(!projList) return

    const threshold = Math.floor(distributionBitLimit32  * dimension.samplingRatio)
    let query               = SELECT + projList + FROM + crossfilter._tablesStmt,
        filterQuery         = '',
        nonNullFilterCount  = 0,
        allFilters          = crossfilter.filters.concat(crossfilter.globalFilters)

    // we observe this dimensions filter
    allFilters.forEach((allFilter) => {
        if (allFilter && allFilter !== '') {
            if (nonNullFilterCount > 0) {
                filterQuery += AND
            }
            nonNullFilterCount++
            filterQuery += allFilter
        }
    })
    if (dimension._selfFilter) {
        if (filterQuery !== '') {
            filterQuery += AND + dimension._selfFilter
        } else {
            filterQuery = dimension._selfFilter
        }
    }
    if (filterQuery !== '') {
        query += WHERE + filterQuery
    }
    if (dimension.samplingRatio !== null && dimension.samplingRatio < 1.0) {
        if (filterQuery) {
            query += AND
        } else {
            query += WHERE
        }
        query += " MOD(" + rowIdAttr + " * " + knuthHash + ", " + distributionBitLimit32 + ") < " + threshold
    }
    if (crossfilter._joinStmt !== null) {
        if (filterQuery === '' && (dimension.samplingRatio === null || dimension.samplingRatio >= 1.0)) {
            query += WHERE
        } else {
            query += AND
        }
        query += crossfilter._joinStmt
    }
    return isRelative(query) ? replaceRelative(query) : query
}

function writeTopBottomQuery(dimension, k, offset, ascDescExpr, isRender) {
    let query = writeQuery(!!isRender);
    if (!query) return ''

    if (dimension._orderExpression) { // overrides any other ordering based on dimension
        query += ORDER_BY + dimension._orderExpression + ascDescExpr
    } else if (dimension.dimensionExpression)  {
        query += ORDER_BY + dimension.dimensionExpression + ascDescExpr
    }
    if (k !== Infinity) {
        query += LIMIT + k
    }
    if (offset !== undefined) {
        query += OFFSET + offset
    }
    return query
}

function writeTopQuery(dimension, k, offset, isRender) {
    return writeTopBottomQuery(dimension, k, offset, DESC, isRender);
}

function top(crossfilter, dimension, k, offset, renderSpec, callback) {
    if (!callback) console.warn("Warning: Deprecated sync method dimension.top(). Please use async version");

    const query = writeTopQuery(dimension, k, offset, !!renderSpec)
    if (!query) {
        if (callback) {
            // TODO(croot): throw an error instead?
            callback(null, {})
            return
        }
        return {}
    }
    const options = getQueryOptions(dimension, renderSpec)
    return callback ? cache.queryAsync(query, options, callback) : crossfilter.cache.query(query, options)
}

function writeBottomQuery(dimension, k, offset, isRender) {
    return writeTopBottomQuery(dimension, k, offset, ASC, isRender);
}

function bottom(crossfilter, dimension, k, offset, renderSpec, callback) {
    if (!callback) console.warn("Warning: Deprecated sync method dimension.bottom(). Please use async version")

    const query = writeBottomQuery(dimension, k, offset, !!renderSpec);
    if (!query) {
        if (callback) {
            // TODO(croot): throw an error instead?
            callback(null, {})
            return
        }
        return {}
    }
    const async     = !!callback,
          options   = getQueryOptions(dimension, renderSpec)
    return callback ? cache.queryAsync(query, options, callback) : crossfilter.cache.query(query, options)
}

function getQueryOptions(dimension, renderSpec) {
    return {
        eliminateNullRows: dimension._eliminateNull,
        renderSpec       : renderSpec,
        postProcessors   : null,
        queryId          : dimension._dimensionIndex
    }
}