import {isRelative, replaceRelative} from "../group/Filter"

const SELECT    = "SELECT ",
  FROM      = " FROM ",
  WHERE     = " WHERE ",
  AND       = " AND ",
  ORDER_BY  = " ORDER BY ",
  ASC       = " ASC",
  DESC      = " DESC",
  LIMIT     = " LIMIT ",
  OFFSET    = " OFFSET "

/************   sql fragments from other files/classes   *************************
 *
 */
// https://lowrey.me/exploring-knuths-multiplicative-hash-2/
const KNUTH_HASH                = "265445761" // knuthMultiplicativeHash
const DISTRIBUTION_BIT_LIMIT_32 = "4294967296"

export function convertDimensionArraysToString(crossfilter, dimension, hasRenderSpec, rowIdAttr) {
  let projList = ""
  const { _projectOnAllDimensionsFlag, _projectExpressions } = dimension
  if (_projectOnAllDimensionsFlag) {
    const dimensions        = crossfilter.getDimensions()
    let nonNullDimensions   = [],
      dimSet                = {}

    dimensions.forEach((dimensionItem) => {
      if(dimensionItem !== null && dimensionItem !== "") {
        nonNullDimensions.push(dimensionItem)
      }
    })

    nonNullDimensions = nonNullDimensions.concat(_projectExpressions)
    // now make set of unique non null dimensions
    nonNullDimensions.forEach((nonNullDimension) => {
      if (!(nonNullDimension in dimSet)) {
        dimSet[nonNullDimension] = null
      }
    })
    nonNullDimensions = []
    Reflect.ownKeys(dimSet).map((key) => {
      nonNullDimensions.push(key)
    })

    projList = nonNullDimensions.join(",")
  } else {
    projList = _projectExpressions.join(",")
  }
  if (hasRenderSpec) {
    if (projList.indexOf("rowid") < 0 && projList.indexOf(rowIdAttr) < 0) {
      projList += "," + rowIdAttr
    }
  }
  return projList === "" ? false : projList
}

// Returns the top K selected records based on this dimension"s order.
// Note: observes this dimension"s filter, unlike group and groupAll.
function writeQuery(dimension, hasRenderSpec) {
  const crossfilter                                        = dimension.getCrossfilter(),
    { _tablesStmt, _globalFilters, _filters, _joinStmt } = crossfilter,
    { _samplingRatio, _selfFilter }                      = dimension,
    rowIdAttr                                            = crossfilter.getDataTables()[0] + ".rowid"
  let projList = convertDimensionArraysToString(crossfilter, dimension, hasRenderSpec, rowIdAttr)
  // stops query from happening if variables do not exist in chart
  if(!projList) return
  const threshold = Math.floor(DISTRIBUTION_BIT_LIMIT_32  * _samplingRatio)
  let query             = SELECT + projList + FROM + _tablesStmt,
    filterQuery         = "",
    nonNullFilterCount  = 0,
    allFilters          = _filters.concat(_globalFilters)

  // we observe this dimensions filter
  allFilters.forEach((allFilter) => {
    if (allFilter && allFilter !== "") {
      if (nonNullFilterCount > 0) {
        filterQuery += AND
      }
      nonNullFilterCount++
      filterQuery += allFilter
    }
  })
  if (_selfFilter) {
    if (filterQuery !== "") {
      filterQuery += AND + _selfFilter
    } else {
      filterQuery = _selfFilter
    }
  }
  if (filterQuery !== "") {
    query += WHERE + filterQuery
  }
  if (_samplingRatio !== null && _samplingRatio < 1.0) {
    if (filterQuery) {
      query += AND
    } else {
      query += WHERE
    }
    query += " MOD(" + rowIdAttr + " * " + KNUTH_HASH + ", " + DISTRIBUTION_BIT_LIMIT_32 + ") < " + threshold
  }
  if (_joinStmt !== null) {
    if (filterQuery === "" && (_samplingRatio === null || _samplingRatio >= 1.0)) {
      query += WHERE
    } else {
      query += AND
    }
    query += _joinStmt
  }
  return isRelative(query) ? replaceRelative(query) : query
}
export function top(dimension, k, offset, renderSpec, callback) {
  if (!callback) console.warn("Warning: Deprecated sync method dimension.top(). Please use async version")

  const query = writeTopQuery(dimension, k, offset, !!renderSpec)
  if (!query) {
    if (callback) {
      callback(null, {})
      return
    }
    return {}
  }
  const options = getQueryOptions(dimension, renderSpec)
  return callback ? dimension._cache.queryAsync(query, options, callback) : dimension._cache.query(query, options)
}
export function topAsync (dimension, k, offset, renderSpec) {
  return new Promise((resolve, reject) => {
    top(dimension, k, offset, renderSpec, (error, result) => {
      if (error) {
        reject(error)
      } else {
        resolve(result)
      }
    })
  })
}
export function writeTopQuery(dimension, k, offset, isRender) {
  return writeTopBottomQuery(dimension, k, offset, DESC, isRender)
}
export function writeTopBottomQuery(dimension, k, offset, ascDescExpr, isRender) {
  let query = writeQuery(dimension, !!isRender)
  if (!query) return ''

  if (dimension._orderExpression) { // overrides any other ordering based on dimension
    query += ORDER_BY + dimension._orderExpression + ascDescExpr
  } else if (dimension._dimensionExpression)  {
    query += ORDER_BY + dimension._dimensionExpression + ascDescExpr
  }
  if (k !== Infinity) {
    query += LIMIT + k
  }
  if (offset !== undefined) {
    query += OFFSET + offset
  }
  return query
}
export function bottom(dimension, k, offset, renderSpec, callback) {
  if (!callback) console.warn("Warning: Deprecated sync method dimension.bottom(). Please use async version")

  const query = writeBottomQuery(dimension, k, offset, !!renderSpec)
  if (!query) {
    if (callback) {
      callback(null, {})
      return
    }
    return {}
  }
  const async = !!callback,
    options   = getQueryOptions(dimension, renderSpec)
  return callback ? dimension._cache.queryAsync(query, options, callback) : dimension._cache.query(query, options)
}
export function writeBottomQuery(dimension, k, offset, isRender) {
  return writeTopBottomQuery(dimension, k, offset, ASC, isRender)
}
export function getQueryOptions(dimension, renderSpec) {
  return {
    eliminateNullRows: dimension._eliminateNull,
    renderSpec       : renderSpec,
    postProcessors   : null,
    queryId          : dimension._dimensionIndex
  }
}
