import {
  checkIfTimeBinInRange,
  formatDateResult,
  autoBinParams,
  unBinResults
} from "./modules/binning"
import { sizeAsyncWithEffects, sizeSyncWithEffects } from "./modules/group"
import moment from "moment"

Array.prototype.includes =
  Array.prototype.includes ||
  function(searchElement, fromIndex) {
    return this.slice(fromIndex || 0).indexOf(searchElement) >= 0
  }

function filterNullMeasures(filterStatement, measures) {
  var measureNames = measures
    .filter(notEmptyNotStarNotComposite)
    .map(toProp("expression"))
  var maybeParseParameters = flatten(measureNames.map(parseParensIfExist))
  var nullColumnsFilter = maybeParseParameters.map(isNotNull).join(" AND ")
  var newfilterStatement = maybeAnd(filterStatement, nullColumnsFilter)
  return newfilterStatement
}
function toProp(propName) {
  return function(item) {
    return item[propName]
  }
}
function isNotNull(columnName) {
  return columnName + " IS NOT NULL"
}
function notEmptyNotStarNotComposite(item) {
  return (
    notEmpty(item.expression) && item.expression !== "*" && !item.isComposite
  )
}

function parseParensIfExist(measureValue) {
  // slightly hacky regex, but goes down for 4 levels deep in terms of nesting ().
  var checkParens = /\(([^()]*|\(([^()]*|\(([^()]*|\([^()]*\))*\))*\))*\)/g
  var thereIsParens = checkParens.test(measureValue)

  if (thereIsParens) {
    var parsedParens = measureValue.match(checkParens)
    return parsedParens.map(function(str) {
      return str.slice(1, -1)
    })
  } else {
    return [measureValue]
  }
}
function flatten(arr) {
  return arr.reduce(function(flat, toFlatten) {
    return flat.concat(
      Array.isArray(toFlatten) ? flatten(toFlatten) : toFlatten
    )
  }, [])
}

function notEmpty(item) {
  switch (typeof item) {
    case "undefined":
      return false
    case "boolean":
      return true
    case "number":
      return true
    case "symbol":
      return true
    case "function":
      return true
    case "string":
      return item.length > 0

    // null, array, object, date
    case "object":
      return (
        item !== null &&
        (typeof item.getDay === "function" || Object.keys(item).length > 0)
      ) // jscs:ignore maximumLineLength
  }
}

/**
 * helper function for creating WKT polygon string to validate points
 * @param pointsArr
 * @returns {boolean}
 */
function isValidPointsArray(pointsArr) {
  if(Array.isArray(pointsArr) && pointsArr.length > 1) {

    function isPointValid(point) {
      if(Array.isArray(point) && point.length === 2) {
        return point.every(coord => typeof coord === "number")
      } else {
        return false
      }
    }

    return pointsArr.every(isPointValid)
  }
  else {
    return false;
  }
}

/**
 * creates WKT POLYGON string from given points array
 * @param points, ex: [[-180,-90], [180.-90], [180,90], [-180,90]]
 * @returns {string}
 */
export function createWKTPolygonFromPoints(pointsArr) {
  if(isValidPointsArray(pointsArr)) {
    let wkt_str = "POLYGON(("
    pointsArr.forEach((p) => {
      wkt_str += `${p[0]} ${p[1]}, `
    })
    wkt_str += `${pointsArr[0][0]} ${pointsArr[0][1]}))`
    return wkt_str
  } else {
    return false;
  }

}

function maybeAnd(clause1, clause2) {
  var joiningWord = clause1 === "" || clause2 === "" ? "" : " AND "
  return clause1 + joiningWord + clause2
}

function _mapColumnsToNameAndType(columns) {
  return Object.keys(columns).map(function(key) {
    var col = columns[key]
    return { rawColumn: key, column: col.column, type: col.type }
  })
}

function _findIndexOfColumn(columns, targetColumn) {
  return columns.reduce(function(colIndex, col, i) {
    var containsField =
      col.rawColumn === targetColumn || col.column === targetColumn
    if (colIndex === -1 && containsField) {
      colIndex = i
    }
    return colIndex
  }, -1)
}

function _isDateField(field) {
  return field.type === "DATE"
}

var TYPES = {
  undefined: "undefined",
  number: "number",
  boolean: "boolean",
  string: "string",
  "[object Function]": "function",
  "[object RegExp]": "regexp",
  "[object Array]": "array",
  "[object Date]": "date",
  "[object Error]": "error"
}

var TOSTRING = Object.prototype.toString

function type(o) {
  return TYPES[typeof o] || TYPES[TOSTRING.call(o)] || (o ? "object" : "null")
}

function formatFilterValue(value, wrapInQuotes, isExact) {
  var valueType = type(value)
  if (valueType == "string") {
    var escapedValue = value.replace(/'/g, "''")

    if (!isExact) {
      escapedValue = escapedValue.replace(/%/g, "\\%")
      escapedValue = escapedValue.replace(/_/g, "\\_")
    }

    return wrapInQuotes ? "'" + escapedValue + "'" : escapedValue
  } else if (valueType == "date") {
    return (
      "TIMESTAMP(0) '" +
      value
        .toISOString()
        .slice(0, 19)
        .replace("T", " ") +
      "'"
    )
  } else {
    return value
  }
}

function pruneCache(allCacheResults) {
  return allCacheResults.reduce((cacheArr, cache) => {
    if (notEmpty(cache.peekAtCache().cache)) {
      return cacheArr.concat(cache)
    } else {
      return cacheArr
    }
  }, [])
}

function uncast(string) {
  const matching = string.match(/^CAST\([\w_]{0,250}/)
  if (matching) {
    return matching[0].split("CAST(")[1]
  } else {
    return string
  }
}

function isRelative(sqlStr) {
  return /DATE_ADD\(([^,|.]+), (DATEDIFF\(\w+, ?\d+, ?\w+\(\)\)[-+0-9]*|[-0-9]+), ([0-9]+|NOW\(\))\)|NOW\(\)/g.test(
    sqlStr
  )
}

export function replaceRelative(sqlStr) {
  const relativeDateRegex = /DATE_ADD\(([^,|.]+), (DATEDIFF\(\w+, ?\d+, ?\w+\(\)\)[-+0-9]*|[-0-9]+), ([0-9]+|NOW\(\))\)/g
  const withRelative = sqlStr.replace(
    relativeDateRegex,
    (match, datepart, number, date) => {
      if (isNaN(number)) {
        const num = Number(number.slice(number.lastIndexOf(")") + 1))
        if (isNaN(num)) {
          return formatFilterValue(
            moment()
              .utc()
              .startOf(datepart)
              .toDate(),
            true
          )
        } else {
          return formatFilterValue(
            moment()
              .add(num, datepart)
              .utc()
              .startOf(datepart)
              .toDate(),
            true
          )
        }
      } else {
        return formatFilterValue(
          moment()
            .add(number, datepart)
            .toDate(),
          true
        )
      }
    }
  )
  const withNow = withRelative.replace(
    /NOW\(\)/g,
    formatFilterValue(moment().toDate(), true)
  )
  return withNow
}

;(function(exports) {
  crossfilter.version = "1.3.11"
  exports.resultCache = resultCache
  exports.crossfilter = crossfilter
  exports.filterNullMeasures = filterNullMeasures
  exports.notEmpty = notEmpty
  exports.parseParensIfExist = parseParensIfExist
  exports.unBinResults = unBinResults

  let allResultCache = []

  var CF_ID = 0 // crossfilter id

  function resultCache(con) {
    var resultCache = {
      query: query,
      queryAsync: queryAsync,
      emptyCache: emptyCache,
      setMaxCacheSize: function(size) {
        maxCacheSize = size
      },
      setDataConnector: function(con) {
        _dataConnector = con
      },
      peekAtCache: function() {
        return { cache: cache, emptyCache: emptyCache }
      }, // TODO 4 test
      getMaxCacheSize: function() {
        return maxCacheSize
      }, // TODO test only
      getDataConnector: function() {
        return _dataConnector
      } // TODO test only
    }

    var maxCacheSize = 10 // TODO should be top-level constant or init param
    var cache = {}
    var cacheCounter = 0
    var _dataConnector = null // TODO con not used elsewhere

    function evictOldestCacheEntry() {
      var oldestQuery = null
      var lowestCounter = Number.MAX_SAFE_INTEGER
      for (var key in cache) {
        if (cache[key].time < lowestCounter) {
          oldestQuery = key
          lowestCounter = cache[key].time
        }
      }
      delete cache[oldestQuery]
    }

    function emptyCache() {
      cache = {}
      return resultCache
    }

    function queryAsync(query, options, callback) {
      var eliminateNullRows = false
      var renderSpec = null
      var postProcessors = null
      var queryId = null
      if (options) {
        eliminateNullRows = options.eliminateNullRows
          ? options.eliminateNullRows
          : false
        renderSpec = options.renderSpec ? options.renderSpec : null
        postProcessors = options.postProcessors ? options.postProcessors : null
        queryId = options.queryId ? options.queryId : null
      }

      var numKeys = Object.keys(cache).length

      if (!renderSpec) {
        if (query in cache && cache[query].showNulls === eliminateNullRows) {
          cache[query].time = cacheCounter++

          // change selector to null as it should already be in cache
          // no postProcessors, shouldCache: true
          asyncCallback(
            query,
            null,
            !renderSpec,
            cache[query].data,
            eliminateNullRows,
            callback
          )
          return
        }
        if (numKeys >= maxCacheSize) {
          // should never be gt
          evictOldestCacheEntry() // TODO only reachable if query not in cache
        }
      }

      var conQueryOptions = {
        columnarResults: true,
        eliminateNullRows: eliminateNullRows,
        renderSpec: renderSpec,
        queryId: queryId
      }

      return _dataConnector.query(query, conQueryOptions, function(
        error,
        result
      ) {
        if (error) {
          callback(error)
        } else {
          asyncCallback(
            query,
            postProcessors,
            !renderSpec,
            result,
            eliminateNullRows,
            callback
          )
        }
      })
    }

    function asyncCallback(
      query,
      postProcessors,
      shouldCache,
      result,
      showNulls,
      callback
    ) {
      if (!shouldCache) {
        if (!postProcessors) {
          callback(null, result)
        } else {
          var data = result
          for (var s = 0; s < postProcessors.length; s++) {
            data = postProcessors[s](result)
          }
          callback(null, data)
        }
      } else {
        if (!postProcessors) {
          cache[query] = {
            time: cacheCounter++,
            data: result,
            showNulls: showNulls
          }
        } else {
          var data = result
          for (var s = 0; s < postProcessors.length; s++) {
            data = postProcessors[s](data)
          }
          cache[query] = {
            time: cacheCounter++,
            data: data,
            showNulls: showNulls
          }
        }

        callback(null, cache[query].data)
      }
    }

    function query(query, options) {
      var eliminateNullRows = false
      var renderSpec = null
      var postProcessors = null
      var queryId = null
      if (options) {
        eliminateNullRows = options.eliminateNullRows
          ? options.eliminateNullRows
          : false
        renderSpec = options.renderSpec ? options.renderSpec : null
        postProcessors = options.postProcessors ? options.postProcessors : null
        queryId = options.queryId ? options.queryId : null
      }

      var numKeys = Object.keys(cache).length

      if (!renderSpec) {
        if (query in cache && cache[query].showNulls === eliminateNullRows) {
          cache[query].time = cacheCounter++
          return cache[query].data
        }
        if (numKeys >= maxCacheSize) {
          // should never be gt
          evictOldestCacheEntry()
        }
      }
      var data = null
      var conQueryOptions = {
        columnarResults: true,
        eliminateNullRows: eliminateNullRows,
        renderSpec: renderSpec,
        queryId: queryId
      }

      if (!postProcessors) {
        data = _dataConnector.query(query, conQueryOptions)
        if (!renderSpec) {
          cache[query] = {
            time: cacheCounter++,
            data: data,
            showNulls: eliminateNullRows
          }
        }
      } else {
        data = _dataConnector.query(query, conQueryOptions)
        for (var s = 0; s < postProcessors.length; s++) {
          data = postProcessors[s](data)
        }
        if (!renderSpec) {
          cache[query] = {
            time: cacheCounter++,
            data: data,
            showNulls: eliminateNullRows
          }
        }
      }

      return data
    }

    _dataConnector = con // TODO unnecessary
    allResultCache.push(resultCache)
    return resultCache
  }

  function crossfilter() {
    var crossfilter = {
      type: "crossfilter",
      setDataAsync: setDataAsync,
      filter: filter,
      getColumns: getColumns,
      dimension: dimension,
      groupAll: groupAll,
      size: size,
      sizeAsync: sizeAsync,
      getId: function() {
        return _id
      },
      getFilter: function() {
        return filters
      },
      setGlobalFilter: setGlobalFilter,
      getGlobalFilter: function() {
        return globalFilters
      },
      getFilterString: getFilterString,
      getGlobalFilterString: getGlobalFilterString,
      getDimensions: function() {
        return dimensions
      },
      getTable: function() {
        return _dataTables
      },
      peekAtCache: function() {
        return cache.peekAtCache()
      },
      clearAllResultCaches: function() {
        allResultCache = pruneCache(allResultCache)
        allResultCache.forEach(resultCache => {
          resultCache.emptyCache()
        })
      }
    }

    var _dataTables = null
    var _joinAttrMap = {}
    var _joinStmt = null
    var _tablesStmt = null
    var filters = []
    var targetFilter = null
    var columnTypeMap = null
    var compoundColumnMap = null
    var _dataConnector = null
    var dimensions = []
    var globalFilters = []
    var cache = null
    var _id = CF_ID++

    function getFieldsPromise(table) {
      return new Promise((resolve, reject) => {
        _dataConnector.getFields(table, (error, columnsArray) => {
          if (error) {
            reject(error)
          } else {
            var columnNameCountMap = {}

            columnsArray.forEach(function(element) {
              var compoundName = table + "." + element.name
              columnTypeMap[compoundName] = {
                table: table,
                column: element.name,
                type: element.type,
                precision: element.precision,
                is_array: element.is_array,
                is_dict: element.is_dict,
                name_is_ambiguous: false
              }
              columnNameCountMap[element.name] =
                columnNameCountMap[element.name] === undefined
                  ? 1
                  : columnNameCountMap[element.name] + 1
            })

            for (var key in columnTypeMap) {
              if (columnNameCountMap[columnTypeMap[key].column] > 1) {
                columnTypeMap[key].name_is_ambiguous = true
              } else {
                compoundColumnMap[columnTypeMap[key].column] = key
              }
            }
            resolve(crossfilter)
          }
        })
      })
    }

    function setDataAsync(dataConnector, dataTables, joinAttrs) {
      /* joinAttrs should be an array of objects with keys
       * table1, table2, attr1, attr2
       */
      _dataConnector = dataConnector
      cache = resultCache(_dataConnector)
      _dataTables = dataTables
      if (!Array.isArray(_dataTables)) {
        _dataTables = [_dataTables]
      }
      _tablesStmt = ""
      _dataTables.forEach(function(table, i) {
        if (i > 0) {
          _tablesStmt += ","
        }
        _tablesStmt += table
      })
      _joinStmt = null
      if (typeof joinAttrs !== "undefined") {
        _joinAttrMap = {}
        _joinStmt = ""
        joinAttrs.forEach(function(join, i) {
          var joinKey =
            join.table1 < join.table2
              ? join.table1 + "." + join.table2
              : join.table2 + "." + join.table1
          var tableJoinStmt =
            join.table1 +
            "." +
            join.attr1 +
            " = " +
            join.table2 +
            "." +
            join.attr2
          if (i > 0) {
            _joinStmt += " AND "
          }
          _joinStmt += tableJoinStmt
          _joinAttrMap[joinKey] = tableJoinStmt
        })
      }
      columnTypeMap = {}
      compoundColumnMap = {}

      return Promise.all(_dataTables.map(getFieldsPromise)).then(
        () => crossfilter
      )
    }

    function getColumns() {
      return columnTypeMap
    }

    function setGlobalFilter(setter) {
      if (typeof setter === "function") {
        globalFilters = setter(globalFilters)
      } else if (Array.isArray(setter)) {
        globalFilters = setter
      } else {
        throw new Error("Invalid argument. Must be function or array")
      }
    }

    function getFilterString(dimIgnoreIndex = -1) {
      // index of dimension's filters to ignore
      var filterString = ""
      var firstElem = true
      filters.forEach(function(value, index) {
        if (value != null && value != "" && index !== dimIgnoreIndex) {
          if (!firstElem) {
            filterString += " AND "
          }
          firstElem = false
          filterString += value
        }
      })
      return isRelative(filterString)
        ? replaceRelative(filterString)
        : filterString
    }

    function getGlobalFilterString() {
      var filterString = ""
      var firstElem = true
      globalFilters.forEach(function(value) {
        if (value != null && value != "") {
          if (!firstElem) {
            filterString += " AND "
          }
          firstElem = false
          filterString += value
        }
      })
      return isRelative(filterString)
        ? replaceRelative(filterString)
        : filterString
    }

    function filter(isGlobal) {
      var filter = {
        filter: filter,
        filterAll: filterAll,
        getFilter: getFilter,
        toggleTarget: toggleTarget,
        getTargetFilter: function() {
          return targetFilter
        } // TODO for test only
      }

      var filterIndex

      if (isGlobal) {
        filterIndex = globalFilters.length
        globalFilters.push("")
      } else {
        filterIndex = filters.length
        filters.push("")
      }

      function toggleTarget() {
        if (targetFilter == filterIndex) {
          targetFilter = null
        } else {
          targetFilter = filterIndex
        }
        return filter
      }

      function getFilter() {
        if (isGlobal) {
          return globalFilters[filterIndex]
        }

        return filters[filterIndex]
      }

      function filter(filterExpr) {
        if (filterExpr == undefined || filterExpr == null) {
          filterAll()
        } else if (isGlobal) {
          globalFilters[filterIndex] = filterExpr
        } else {
          filters[filterIndex] = filterExpr
        }
        return filter
      }

      function filterAll() {
        if (isGlobal) {
          globalFilters[filterIndex] = ""
        } else {
          filters[filterIndex] = ""
        }
        return filter
      }

      return filter
    }

    function dimension(expression, isGlobal) {
      var dimension = {
        type: "dimension",
        order: order,
        nullsOrder: nullsOrder,
        orderNatural: orderNatural,
        selfFilter: selfFilter,
        filter: filter,
        filterRelative: filterRelative,
        filterExact: filterExact,
        filterRange: filterRange,
        filterST_Contains: filterST_Contains,
        filterST_Intersects: filterST_Intersects,
        filterAll: filterAll,
        filterMulti: filterMulti,
        filterLike: filterLike,
        filterILike: filterILike,
        filterNotEquals: filterNotEquals,
        filterNotLike: filterNotLike,
        filterNotILike: filterNotILike,
        filterIsNotNull: filterIsNotNull,
        filterIsNull: filterIsNull,
        getCrossfilter: function() {
          return crossfilter
        },
        getDimensionIndex: function() {
          return dimensionIndex
        },
        getCrossfilterId: crossfilter.getId,
        getFilter: getFilter,
        getFilterString: getFilterString,
        projectOn: projectOn,
        getProjectOn: function() {
          return projectExpressions
        },
        getTable: crossfilter.getTable,
        projectOnAllDimensions: projectOnAllDimensions,
        samplingRatio: samplingRatio,
        top: top,
        topAsync: topAsync,
        bottom: bottom,
        bottomAsync: bottom,
        group: group,
        groupAll: groupAll,
        toggleTarget: toggleTarget,
        removeTarget: removeTarget,
        allowTargeted: allowTargeted,
        isTargeting: isTargeting,
        dispose: dispose,
        remove: dispose,
        writeTopQuery: writeTopQuery,
        writeBottomQuery: writeBottomQuery,
        value: function() {
          return dimArray
        },
        set: function(fn) {
          dimArray = fn(dimArray)
          return dimension
        },

        // makes filter conjunctive
        setDrillDownFilter: function(v) {
          drillDownFilter = v
          return dimension
        },

        getSamplingRatio: function() {
          return samplingRatio
        }, // TODO for tests only
        multiDim: multiDim,
        setEliminateNull: function(v) {
          eliminateNull = v
          return dimension
        },
        getEliminateNull: function() {
          return eliminateNull
        }, // TODO test only
        getDimensionName: function() {
          return dimensionName
        }
      }
      var filterVal = null
      var _allowTargeted = true
      var _selfFilter = null
      var dimensionIndex = isGlobal ? globalFilters.length : filters.length
      var scopedFilters = isGlobal ? globalFilters : filters
      var dimensionGroups = []
      var _orderExpression = null
      scopedFilters.push("")
      var projectExpressions = []
      var projectOnAllDimensionsFlag = false
      var binBounds = null // for binning
      var rangeFilters = []
      var dimContainsArray = []
      var eliminateNull = true
      var _nullsOrder = ""

      // option for array columns
      // - means observe own filter and use conjunctive instead of disjunctive between sub-filters
      var drillDownFilter = false
      var cache = resultCache(_dataConnector)
      var dimensionExpression = null
      var samplingRatio = null

      var expression = Array.isArray(expression) ? expression : [expression]

      var isMultiDim = expression.length > 1
      var columns = _mapColumnsToNameAndType(crossfilter.getColumns())
      var dimArray = expression.map(function(field) {
        var indexOfColumn = _findIndexOfColumn(columns, field)
        var isDate = indexOfColumn > -1 && _isDateField(columns[indexOfColumn])
        if (isDate) {
          field = "CAST(" + field + " AS TIMESTAMP(0))"
        }
        return field
      })
      var dimensionName = expression.map(function(field) {
        return field
      })
      dimensionExpression = dimArray.includes(null) ? null : dimArray.join(", ")

      function nullsOrder(str) {
        if (!arguments.length) {
          return _nullsOrder
        }
        _nullsOrder = str
        return dimension
      }
      function multiDim(value) {
        if (typeof value === "boolean") {
          isMultiDim = value
          return dimension
        }

        return isMultiDim
      }

      function order(orderExpression) {
        _orderExpression = orderExpression
        return dimension
      }

      function orderNatural() {
        _orderExpression = null
        return dimension
      }

      function selfFilter(_) {
        if (!arguments.length) return _selfFilter
        _selfFilter = _
        return dimension
      }

      function allowTargeted(allowTargeted) {
        if (!arguments.length) {
          return _allowTargeted
        }
        _allowTargeted = allowTargeted
        return dimension
      }

      function toggleTarget() {
        if (targetFilter == dimensionIndex) {
          // TODO duplicates isTargeting
          targetFilter = null // TODO duplicates removeTarget
        } else {
          targetFilter = dimensionIndex
        }
      }

      function removeTarget() {
        if (targetFilter == dimensionIndex) {
          targetFilter = null
        }
      }

      function isTargeting() {
        return targetFilter == dimensionIndex
      }

      function projectOn(expressions) {
        projectExpressions = expressions
        return dimension
      }

      function projectOnAllDimensions(flag) {
        projectOnAllDimensionsFlag = flag
        return dimension
      }

      function getFilter() {
        return filterVal
      }

      function getFilterString() {
        return scopedFilters[dimensionIndex]
      }

      function filter(
        range,
        append = false,
        resetRange,
        inverseFilter,
        binParams = [{ extract: false }]
      ) {
        if (typeof range == "undefined") {
          return filterAll()
        } else if (Array.isArray(range) && !isMultiDim) {
          return filterRange(
            range,
            append,
            resetRange,
            inverseFilter,
            binParams
          )
        } else {
          return filterExact(range, append, inverseFilter, binParams)
        }
      }

      function filterRelative(
        range,
        append = false,
        resetRange,
        inverseFilter
      ) {
        return filterRange(range, append, resetRange, inverseFilter, null, true)
      }

      function filterExact(value, append, inverseFilter, binParams = []) {
        value = Array.isArray(value) ? value : [value]
        var subExpression = ""
        for (var e = 0; e < value.length; e++) {
          if (e > 0) {
            subExpression += " AND "
          }
          var typedValue = formatFilterValue(value[e], true, true)
          if (dimContainsArray[e]) {
            subExpression += typedValue + " = ANY " + dimArray[e]
          } else if (Array.isArray(typedValue)) {
            if (typedValue[0] instanceof Date) {
              const min = formatFilterValue(typedValue[0])
              const max = formatFilterValue(typedValue[1])
              const dimension = dimArray[e]
              subExpression +=
                dimension + " >= " + min + " AND " + dimension + " <= " + max
            } else {
              const min = typedValue[0]
              const max = typedValue[1]
              const dimension = dimArray[e]
              subExpression +=
                dimension + " >= " + min + " AND " + dimension + " <= " + max
            }
          } else {
            if (binParams[e] && binParams[e].extract) {
              subExpression +=
                "extract(" +
                binParams[e].timeBin +
                " from " +
                uncast(dimArray[e]) +
                ") = " +
                typedValue
            } else {
              subExpression +=
                typedValue === null
                  ? `${dimArray[e]} IS NULL`
                  : `${dimArray[e]} = ${typedValue}`
            }
          }
        }
        if (inverseFilter) {
          subExpression = "NOT (" + subExpression + ")"
        }

        if (append) {
          scopedFilters[dimensionIndex] += subExpression
        } else {
          scopedFilters[dimensionIndex] = subExpression
        }
        return dimension
      }

      function formNotEqualsExpression(value) {
        var escaped = formatFilterValue(value, true, true)
        return dimensionExpression + " <> " + escaped
      }

      function filterNotEquals(value, append) {
        var escaped = formatFilterValue(value, false, false)
        if (append) {
          scopedFilters[dimensionIndex] += formNotEqualsExpression(value)
        } else {
          scopedFilters[dimensionIndex] = formNotEqualsExpression(value)
        }
        return dimension
      }

      function formLikeExpression(value) {
        var escaped = formatFilterValue(value, false, false)
        return dimensionExpression + " like '%" + escaped + "%'"
      }

      function formILikeExpression(value) {
        var escaped = formatFilterValue(value, false, false)
        return dimensionExpression + " ilike '%" + escaped + "%'"
      }

      function filterLike(value, append) {
        if (append) {
          scopedFilters[dimensionIndex] += formLikeExpression(value)
        } else {
          scopedFilters[dimensionIndex] = formLikeExpression(value)
        }
        return dimension
      }

      function filterILike(value, append) {
        if (append) {
          scopedFilters[dimensionIndex] += formILikeExpression(value)
        } else {
          scopedFilters[dimensionIndex] = formILikeExpression(value)
        }
        return dimension
      }

      function filterNotLike(value, append) {
        if (append) {
          scopedFilters[dimensionIndex] +=
            "NOT( " + formLikeExpression(value) + ")"
        } else {
          scopedFilters[dimensionIndex] =
            "NOT( " + formLikeExpression(value) + ")"
        }
        return dimension
      }

      function filterNotILike(value, append) {
        if (append) {
          scopedFilters[dimensionIndex] +=
            "NOT( " + formILikeExpression(value) + ")"
        } else {
          scopedFilters[dimensionIndex] =
            "NOT( " + formILikeExpression(value) + ")"
        }
        return dimension
      }

      function filterIsNotNull(append) {
        if (append) {
          scopedFilters[dimensionIndex] += `${expression} IS NOT NULL`
        } else {
          scopedFilters[dimensionIndex] = `${expression} IS NOT NULL`
        }
        return dimension
      }

      function filterIsNull(append) {
        if (append) {
          scopedFilters[dimensionIndex] += `${expression} IS NULL`
        } else {
          scopedFilters[dimensionIndex] = `${expression} IS NULL`
        }
        return dimension
      }

      function filterRange(
        range,
        append = false,
        resetRange,
        inverseFilters,
        binParams,
        isRelative
      ) {
        var isArray = Array.isArray(range[0]) // TODO semi-risky index
        if (!isArray) {
          range = [range]
        }
        filterVal = range
        var subExpression = ""

        for (var e = 0; e < range.length; e++) {
          if (resetRange === true) {
            rangeFilters[e] = range[e]
          }
          if (e > 0) {
            subExpression += " AND "
          }

          var typedRange = [
            formatFilterValue(range[e][0], true),
            formatFilterValue(range[e][1], true)
          ]

          if (isRelative) {
            typedRange = [
              formatRelativeValue(typedRange[0]),
              formatRelativeValue(typedRange[1])
            ]
          }

          if (binParams && binParams[e] && binParams[e].extract) {
            const dimension =
              "extract(" +
              binParams[e].timeBin +
              " from " +
              uncast(dimArray[e]) +
              ")"

            subExpression +=
              dimension +
              " >= " +
              typedRange[0] +
              " AND " +
              dimension +
              " <= " +
              typedRange[1]
          } else {
            subExpression +=
              dimArray[e] +
              " >= " +
              typedRange[0] +
              " AND " +
              dimArray[e] +
              " <= " +
              typedRange[1]
          }
        }

        if (inverseFilters) {
          subExpression = "NOT(" + subExpression + ")"
        }

        if (append) {
          scopedFilters[dimensionIndex] += "(" + subExpression + ")"
        } else {
          scopedFilters[dimensionIndex] = "(" + subExpression + ")"
        }
        return dimension
      }

      function filterST_Contains(pointsArr) { // [[lon, lat], [lon, lat]] format
        const wktString = createWKTPolygonFromPoints(pointsArr) // creating WKT POLYGON from map extent
        if(wktString) {
          const stContainString = "ST_Contains(ST_GeomFromText(";
          const subExpression = `${stContainString}'${wktString}'), ${_tablesStmt}.${dimension.value()})`

          const polyDim = scopedFilters.filter(filter => {
            if(filter && filter !== null) {
              return filter.includes(subExpression)
            }
          })

          if(Array.isArray(polyDim) && polyDim.length < 1) { // don't use exact same ST_Contains within a vega
            scopedFilters[dimensionIndex] = "(" + subExpression + ")"
          }
        }
        else {
          throw new Error("Invalid points array. Must be array of arrays with valid point coordinates")
        }
        return dimension
      }

      function filterST_Intersects(pointsArr) { // [[lon, lat], [lon, lat]] format
        const wktString = createWKTPolygonFromPoints(pointsArr) // creating WKT POLYGON from map extent
        if(wktString) {
          const stContainString = "ST_Intersects(ST_GeomFromText(";
          const subExpression = `${stContainString}'${wktString}'), ${_tablesStmt}.${dimension.value()})`

          const polyDim = scopedFilters.filter(filter => {
            if(filter && filter !== null) {
              return filter.includes(subExpression)
            }
          })

          if(Array.isArray(polyDim) && polyDim.length < 1) { // don't use exact same ST_Intersects within a vega
            scopedFilters[dimensionIndex] = "(" + subExpression + ")"
          }
        }
        else {
          throw new Error("Invalid points array. Must be array of arrays with valid point coordinates")
        }
        return dimension
      }

      function formatRelativeValue(val) {
        if (val.now) {
          return "NOW()"
        } else if (val.datepart && typeof val.number !== "undefined") {
          const date = typeof val.date !== "undefined" ? val.date : "NOW()"
          const operator =
            typeof val.operator !== "undefined" ? val.operator : "DATE_ADD"
          const number = isNaN(val.number)
            ? formatRelativeValue(val.number)
            : val.number
          const add = typeof val.add !== "undefined" ? val.add : ""
          return `${operator}(${val.datepart}, ${number}, ${date})${add}`
        } else {
          return val
        }
      }

      function filterMulti(
        filterArray,
        resetRangeIn,
        inverseFilters,
        binParams
      ) {
        var filterWasNull =
          filters[dimensionIndex] == null || filters[dimensionIndex] == ""
        var resetRange = false
        if (resetRangeIn !== undefined) {
          resetRange = resetRangeIn
        }

        var lastFilterIndex = filterArray.length - 1
        scopedFilters[dimensionIndex] = "("

        inverseFilters =
          typeof inverseFilters === "undefined" ? false : inverseFilters

        for (var i = 0; i <= lastFilterIndex; i++) {
          var curFilter = filterArray[i]
          filter(curFilter, true, resetRange, inverseFilters, binParams)
          if (i !== lastFilterIndex) {
            if (drillDownFilter ^ inverseFilters) {
              filters[dimensionIndex] += " AND "
            } else {
              filters[dimensionIndex] += " OR "
            }
          }
        }
        scopedFilters[dimensionIndex] += ")"
        return dimension
      }

      function filterAll(softFilterClear) {
        if (softFilterClear == undefined || softFilterClear == false) {
          rangeFilters = []
        }
        filterVal = null
        scopedFilters[dimensionIndex] = ""
        return dimension
      }

      // Returns the top K selected records based on this dimension"s order.
      // Note: observes this dimension"s filter, unlike group and groupAll.
      function writeQuery(hasRenderSpec) {
        var projList = ""
        if (projectOnAllDimensionsFlag) {
          var dimensions = crossfilter.getDimensions()
          var nonNullDimensions = []
          for (var d = 0; d < dimensions.length; d++) {
            // other conditions:
            // && dimensions[d] in columnTypeMap && !columnTypeMap[dimensions[d]].is_array
            if (dimensions[d] !== null && dimensions[d] !== "") {
              nonNullDimensions.push(dimensions[d])
            }
          }
          nonNullDimensions = nonNullDimensions.concat(projectExpressions)
          var dimSet = {}

          // now make set of unique non null dimensions
          for (var d = 0; d < nonNullDimensions.length; d++) {
            if (!(nonNullDimensions[d] in dimSet)) {
              dimSet[nonNullDimensions[d]] = null
            }
          }
          nonNullDimensions = []
          for (var key in dimSet) {
            nonNullDimensions.push(key)
          }
          projList = nonNullDimensions.join(",")
        } else {
          projList = projectExpressions.join(",")
        }

        // stops query from happening if variables do not exist in chart
        if (projList === "") {
          return
        }

        if (hasRenderSpec) {
          var rowIdAttr = _dataTables[0] + ".rowid"
          if (
            projList.indexOf("rowid") < 0 &&
            projList.indexOf(rowIdAttr) < 0
          ) {
            projList += "," + _dataTables[0] + ".rowid"
          }
        }

        var query = "SELECT " + projList + " FROM " + _tablesStmt
        var filterQuery = ""
        var nonNullFilterCount = 0
        var allFilters = filters.concat(globalFilters)

        // we observe this dimensions filter
        for (var i = 0; i < allFilters.length; i++) {
          if (allFilters[i] && allFilters[i] != "") {
            if (nonNullFilterCount > 0) {
              filterQuery += " AND "
            }
            nonNullFilterCount++
            filterQuery += allFilters[i]
          }
        }
        if (_selfFilter) {
          if (filterQuery !== "") {
            filterQuery += " AND " + _selfFilter
          } else {
            filterQuery = _selfFilter
          }
        }
        if (filterQuery !== "") {
          query += " WHERE " + filterQuery
        }
        if (samplingRatio !== null && samplingRatio < 1.0) {
          if (filterQuery) {
            query += " AND "
          } else {
            query += " WHERE "
          }

          // TODO magic numbers
          var threshold = Math.floor(4294967296 * samplingRatio)
          query +=
            " MOD(" +
            _dataTables[0] +
            ".rowid * 265445761, 4294967296) < " +
            threshold
        }
        if (_joinStmt !== null) {
          if (
            filterQuery === "" &&
            (samplingRatio === null || samplingRatio >= 1.0)
          ) {
            query += " WHERE "
          } else {
            query += " AND "
          }
          query += _joinStmt
        }
        return isRelative(query) ? replaceRelative(query) : query
      }

      function samplingRatio(ratio) {
        if (!ratio) samplingRatio = null
        samplingRatio = ratio // TODO always overwrites; typo?
        return dimension
      }

      function writeTopBottomQuery(k, offset, ascDescExpr, isRender) {
        var query = writeQuery(!!isRender)
        if (!query) {
          return ""
        }

        if (_orderExpression) {
          // overrides any other ordering based on dimension
          query += " ORDER BY " + _orderExpression + ascDescExpr + _nullsOrder
        } else if (dimensionExpression) {
          query +=
            " ORDER BY " + dimensionExpression + ascDescExpr + _nullsOrder
        }

        if (k !== Infinity) {
          query += " LIMIT " + k
        }
        if (offset !== undefined) {
          query += " OFFSET " + offset
        }

        return query
      }

      function writeTopQuery(k, offset, isRender) {
        return writeTopBottomQuery(k, offset, " DESC", isRender)
      }

      function top(k, offset, renderSpec, callback) {
        if (!callback) {
          console.warn(
            "Warning: Deprecated sync method dimension.top(). Please use async version"
          )
        }

        var query = writeTopQuery(k, offset, !!renderSpec)
        if (!query) {
          if (callback) {
            // TODO(croot): throw an error instead?
            callback(null, {})
            return
          }
          return {}
        }

        var options = {
          eliminateNullRows: eliminateNull,
          renderSpec: renderSpec,
          postProcessors: null,
          queryId: dimensionIndex
        }

        if (!callback) {
          return cache.query(query, options)
        } else {
          return cache.queryAsync(query, options, callback)
        }
      }

      function topAsync(k, offset, renderSpec) {
        return new Promise((resolve, reject) => {
          top(k, offset, renderSpec, (error, result) => {
            if (error) {
              reject(error)
            } else {
              resolve(result)
            }
          })
        })
      }

      function writeBottomQuery(k, offset, isRender) {
        return writeTopBottomQuery(k, offset, " ASC", isRender)
      }

      function bottom(k, offset, renderSpec, callback) {
        if (!callback) {
          console.warn(
            "Warning: Deprecated sync method dimension.bottom(). Please use async version"
          )
        }

        var query = writeBottomQuery(k, offset, !!renderSpec)
        if (!query) {
          if (callback) {
            // TODO(croot): throw an error instead?
            callback(null, {})
            return
          }
          return {}
        }

        var async = !!callback
        var options = {
          eliminateNullRows: eliminateNull,
          renderSpec: renderSpec,
          postProcessors: null,
          queryId: dimensionIndex
        }

        if (!callback) {
          return cache.query(query, options)
        } else {
          return cache.queryAsync(query, options, callback)
        }
      }

      function group() {
        var group = {
          type: "group",
          order: order,
          orderNatural: orderNatural,
          top: top,
          topAsync: topAsync,
          bottom: bottom,
          bottomAsync: bottom, //deprecated
          all: all,
          allAsync: all, // deprecated
          binParams: binParams,
          setBinParams: binParams,
          reduceCount: reduceCount,
          reduceSum: reduceSum,
          reduceAvg: reduceAvg,
          reduceMin: reduceMin,
          reduceMax: reduceMax,
          reduce: reduce,
          reduceMulti: reduce,
          setBoundByFilter: setBoundByFilter,
          getCrossfilter: function() {
            return crossfilter
          },
          getCrossfilterId: crossfilter.getId,
          getTable: crossfilter.getTable,
          setTargetSlot: function(s) {
            targetSlot = s
          }, // TODO should it return group?
          getTargetSlot: function() {
            return targetSlot
          },
          size: size,
          sizeAsync: sizeAsync,
          writeFilter: writeFilter,
          writeTopQuery: writeTopQuery,
          writeBottomQuery: writeBottomQuery,
          getReduceExpression: function() {
            return reduceExpression
          }, // TODO for testing only
          dimension: function() {
            return dimension
          },

          getProjectOn: getProjectOn,
          getMinMaxWithFilters: minMaxWithFilters
        }
        var reduceExpression = null // count will become default
        var reduceSubExpressions = null
        var reduceVars = null
        var boundByFilter = false
        var dateTruncLevel = null
        var cache = resultCache(_dataConnector)
        var lastTargetFilter = null
        var targetSlot = 0
        var timeParams = null
        var _fillMissingBins = false
        var _orderExpression = null
        var _reduceTableSet = {}
        var _binParams = []

        dimensionGroups.push(group)

        function getProjectOn(isRenderQuery, queryBinParams) {
          var projectExpressions = []
          for (var d = 0; d < dimArray.length; d++) {
            // tableSet[columnTypeMap[dimArray[d]].table] =
            //   (tableSet[columnTypeMap[dimArray[d]].table] || 0) + 1;
            if (
              queryBinParams !== null &&
              queryBinParams !== undefined &&
              typeof queryBinParams[d] !== "undefined" &&
              queryBinParams[d] !== null
            ) {
              var binBounds =
                boundByFilter &&
                typeof rangeFilters[d] !== "undefined" &&
                rangeFilters[d] !== null
                  ? rangeFilters[d]
                  : queryBinParams[d].binBounds
              var binnedExpression = getBinnedDimExpression(
                dimArray[d],
                binBounds,
                queryBinParams[d].numBins,
                queryBinParams[d].timeBin,
                queryBinParams[d].extract
              )
              projectExpressions.push(
                binnedExpression + " as key" + d.toString()
              )
            } else if (dimContainsArray[d]) {
              projectExpressions.push(
                "UNNEST(" + dimArray[d] + ")" + " as key" + d.toString()
              )
            } else if (_binParams && _binParams[d]) {
              var binnedExpression = getBinnedDimExpression(
                dimArray[d],
                _binParams[d].binBounds,
                _binParams[d].numBins,
                _binParams[d].timeBin,
                _binParams[d].extract
              )
              projectExpressions.push(
                binnedExpression + " as key" + d.toString()
              )
            } else {
              if (!!isRenderQuery && dimArray[d].match(/rowid\s*$/)) {
                // do not cast rowid with 'as key[0-9]'
                // as that will mess up hit-test renders
                // and poly renders.
                projectExpressions.push(dimArray[d])
              } else {
                projectExpressions.push(dimArray[d] + " as key" + d.toString())
              }
            }
          }

          if (reduceExpression) {
            projectExpressions.push(reduceExpression)
          }

          return projectExpressions
        }

        function writeFilter(queryBinParams) {
          var filterQuery = ""
          var nonNullFilterCount = 0
          var allFilters = filters.concat(globalFilters)

          // we do not observe this dimensions filter
          for (var i = 0; i < allFilters.length; i++) {
            if (
              (i != dimensionIndex || drillDownFilter == true) &&
              (!_allowTargeted || i != targetFilter) &&
              (allFilters[i] && allFilters[i].length > 0)
            ) {
              // filterQuery != "" is hack as notNullFilterCount was being incremented
              if (nonNullFilterCount > 0 && filterQuery != "") {
                filterQuery += " AND "
              }
              nonNullFilterCount++
              filterQuery += allFilters[i]
            } else if (i == dimensionIndex && queryBinParams != null) {
              var tempBinFilters = ""

              if (nonNullFilterCount > 0) {
                tempBinFilters += " AND "
              }

              nonNullFilterCount++

              var hasBinFilter = false

              for (var d = 0; d < dimArray.length; d++) {
                if (
                  typeof queryBinParams[d] !== "undefined" &&
                  queryBinParams[d] !== null &&
                  !queryBinParams[d].extract
                ) {
                  var queryBounds = queryBinParams[d].binBounds
                  let tempFilterClause = ""
                  if (boundByFilter == true && rangeFilters.length > 0) {
                    queryBounds = rangeFilters[d]
                  }

                  if (d > 0 && hasBinFilter) {
                    tempBinFilters += " AND "
                  }

                  hasBinFilter = true
                  tempFilterClause +=
                    "(" +
                    dimArray[d] +
                    " >= " +
                    formatFilterValue(queryBounds[0], true) +
                    " AND " +
                    dimArray[d] +
                    " <= " +
                    formatFilterValue(queryBounds[1], true) +
                    ")"
                  if (!eliminateNull) {
                    tempFilterClause = `(${tempFilterClause} OR (${
                      dimArray[d]
                    } IS NULL))`
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
            filterQuery += " AND " + _selfFilter
          } else if (_selfFilter && filterQuery == "") {
            filterQuery = _selfFilter
          }
          filterQuery = filterNullMeasures(filterQuery, reduceSubExpressions)
          return isRelative(filterQuery)
            ? replaceRelative(filterQuery)
            : filterQuery
        }

        function getBinnedDimExpression(
          expression,
          binBounds,
          numBins,
          timeBin,
          extract
        ) {
          // jscs:ignore maximumLineLength
          var isDate = type(binBounds[0]) == "date"
          numBins = numBins || 0
          if (isDate) {
            if (timeBin) {
              if (!!extract) {
                return (
                  "extract(" + timeBin + " from " + uncast(expression) + ")"
                )
              } else {
                return "date_trunc(" + timeBin + ", " + expression + ")"
              }
            } else {
              // TODO(croot): throw error if no num bins?
              var dimExpr = "extract(epoch from " + expression + ")"

              // as javscript epoch is in ms
              var filterRange =
                (binBounds[1].getTime() - binBounds[0].getTime()) * 0.001

              var binsPerUnit = numBins / filterRange
              var lowerBoundsUTC = binBounds[0].getTime() / 1000
              const binnedExpression =
                "cast(" +
                "(" +
                dimExpr +
                " - " +
                lowerBoundsUTC +
                ") * " +
                binsPerUnit +
                " as int)"
              return binnedExpression
            }
          } else {
            // TODO(croot): throw error if no num bins?
            var filterRange = binBounds[1] - binBounds[0]

            var binsPerUnit = numBins / filterRange

            if (filterRange === 0) {
              binsPerUnit = 0
            }
            const binnedExpression =
              "cast(" +
              "(cast(" +
              expression +
              " as float) - " +
              binBounds[0] +
              ") * " +
              binsPerUnit +
              " as int)"
            return binnedExpression
          }
        }

        function writeQuery(
          queryBinParams,
          sortByValue,
          ignoreFilters,
          hasRenderSpec
        ) {
          var query = null
          if (
            reduceSubExpressions &&
            (_allowTargeted &&
              (targetFilter !== null || targetFilter !== lastTargetFilter))
          ) {
            reduce(reduceSubExpressions)
            lastTargetFilter = targetFilter
          }

          //var tableSet = {};
          // first clone _reduceTableSet
          //for (key in _reduceTableSet)
          //  tableSet[key] = _reduceTableSet[key];
          query = "SELECT "
          var projectExpressions = getProjectOn(hasRenderSpec, queryBinParams)
          if (!projectExpressions) {
            return ""
          }
          query += projectExpressions.join(",")

          query += checkForSortByAllRows() + " FROM " + _tablesStmt

          function checkForSortByAllRows() {
            // TODO(croot): this could be used as a driver for some kind of
            // scale when rendering, so it should be exposed a better way
            // and returned when getProjectOn() is called.
            return sortByValue === "countval" ? ", COUNT(*) AS countval" : ""
          }

          /*
          //@todo use another method than Object.keys so we don"t break IE8
          var joinTables = _dataTables[0];
          if (Object.keys(tableSet).length === 0)
            query += _dataTables[0];
          else {
            var keyNum = 0;
            joinTables = Object.keys(tableSet);
            for (var k = 0; k < joinTables.length; k++) {
              if (keyNum > 0)
                query += ",";
              keyNum++;
              query += joinTables[k];
            }
          }
          */
          var filterQuery = ignoreFilters ? "" : writeFilter(queryBinParams)
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

          /*
          if (joinTables.length >= 2) {
            if (filterQuery === "")
              query += " WHERE ";
            var joinCount = 0;
            for (var i = 0; i < joinTables.length; i++) {
              for (var j = i + 1; j< joinTables.length; j++) {
                var joinTableKey = joinTables[i] < joinTables[j] ?
                  joinTables[i] + "." + joinTables[j] : joinTables[j] + "." + joinTables[i];
                if (typeof _joinAttrMap[joinTableKey] !== "undefined") {
                  if (joinCount > 0)
                    query += " AND ";
                  query += _joinAttrMap[joinTableKey];
                  joinCount++;
                }
              }
            }
            if (joinCount !== joinTables.length - 1)
              throw ("Invalid join");
          }
          */

          // could use alias "key" here
          query += " GROUP BY "
          for (var i = 0; i < dimArray.length; i++) {
            if (i !== 0) {
              query += ", "
            }

            if (!!hasRenderSpec && dimArray[i].match(/rowid\s*$/)) {
              // do not cast rowid with 'as key[0-9]'
              // as that will mess up hit-test renders
              // and poly renders.
              query += dimArray[i]
            } else {
              query += "key" + i.toString()
            }
          }

          if (queryBinParams !== null) {
            var havingClause = " HAVING "
            var hasBinParams = false
            for (var d = 0; d < queryBinParams.length; d++) {
              if (queryBinParams[d] !== null && !queryBinParams[d].timeBin) {
                let havingSubClause = ""
                if (d > 0 && hasBinParams) {
                  havingClause += " AND "
                }
                hasBinParams = true
                havingSubClause +=
                  "key" +
                  d.toString() +
                  " >= 0 AND key" +
                  d.toString() +
                  " < " +
                  queryBinParams[d].numBins
                if (!eliminateNull) {
                  havingSubClause = `(${havingSubClause} OR key${d.toString()} IS NULL)`
                }
                havingClause += havingSubClause
              }
            }
            if (hasBinParams) {
              query += havingClause
            }
          }

          return query
        }

        function setBoundByFilter(boundByFilterIn) {
          boundByFilter = boundByFilterIn
          return group
        }

        /**
         * Specify an object of binning parameters for each dimension
         * @param {Array<Object>} binParamsIn - Binning parameters for each dimension
         * @param {Number} binParamsIn.numBins - The number of bins for this group
         * @param {Array<Number>} binParamsIn.binBounds - The min and max value bins
         * @param {String} binParamsIn.timeBin - If a time unit, specify the time bin,
         *                                       eg: 'month', year', etc.
         */
        function binParams(binParamsIn) {
          if (!arguments.length) {
            return _binParams
          }

          _binParams = binParamsIn
          return group
        }

        /* istanbul ignore next */
        function fillBins(queryBinParams, results) {
          if (!_fillMissingBins) return results
          var numDimensions = queryBinParams.length
          var numResults = results.length
          var numTimeDims = 0
          for (var d = 0; d < numDimensions; d++) {
            if (queryBinParams[d].timeBin) {
              numTimeDims++
            }
          }
          var filledResults = []

          // we only support filling bins when there is one time dimension
          // and it is the only dimension
          if (numDimensions == 1 && numTimeDims == 1) {
            //@todo fix this
            var actualTimeBinUnit = binParams()[0].timeBin
            var incrementBy = 1

            // convert non-supported time units to moment-compatible inputs
            // http://momentjs.com/docs/#/manipulating/
            switch (actualTimeBinUnit) {
              case "quarterday":
                actualTimeBinUnit = "hours"
                incrementBy = 6
                break
              case "decade":
                actualTimeBinUnit = "years"
                incrementBy = 10
                break
              case "century":
                actualTimeBinUnit = "years"
                incrementBy = 100
                break
              case "millenium":
                actualTimeBinUnit = "years"
                incrementBy = 1000
                break
            }
            var lastResult = null
            var valueKeys = []
            for (var r = 0; r < numResults; r++) {
              var result = results[r]
              if (lastResult) {
                var lastTime = lastResult.key0
                var currentTime = moment(result.key0)
                  .utc()
                  .toDate()
                var nextTimeInterval = moment(lastTime)
                  .utc()
                  .add(incrementBy, actualTimeBinUnit)
                  .toDate()
                var interval = Math.abs(nextTimeInterval - lastTime)
                while (nextTimeInterval < currentTime) {
                  var timeDiff = currentTime - nextTimeInterval
                  if (timeDiff > interval / 2) {
                    // we have a missing time value
                    var insertResult = { key0: nextTimeInterval }
                    for (var k = 0; k < valueKeys.length; k++) {
                      insertResult[valueKeys[k]] = 0
                    }
                    filledResults.push(insertResult)
                  }
                  nextTimeInterval = moment(nextTimeInterval)
                    .utc()
                    .add(incrementBy, actualTimeBinUnit)
                    .toDate()
                }
              } else {
                // first result - get its keys
                var allKeys = Object.keys(result)
                for (var k = 0; k < allKeys.length; k++) {
                  if (allKeys[k] !== "key0") {
                    valueKeys.push(allKeys[k])
                  }
                }
              }
              filledResults.push(result)
              lastResult = result
            }
            return filledResults
          } else if (numTimeDims === 0 && numResults > 0) {
            var allDimsBinned = true // we don't handle for now mixed cases
            var totalArraySize = 1
            var dimensionSizes = []
            var dimensionSums = []
            for (var b = 0; b < queryBinParams.length; b++) {
              if (queryBinParams[b] === null) {
                allDimsBinned = false
                break
              }
              totalArraySize *= queryBinParams[b].numBins
              dimensionSizes.push(queryBinParams[b].numBins)
              dimensionSums.push(
                b == 0 ? 1 : queryBinParams[b].numBins * dimensionSums[b - 1]
              )
            }
            dimensionSums.reverse()
            if (allDimsBinned) {
              var numDimensions = dimensionSizes.length

              // make an array filled with 0 of length numDimensions
              var counters = Array.apply(null, Array(numDimensions)).map(
                Number.prototype.valueOf,
                0
              )
              var allKeys = Object.keys(results[0])
              var valueKeys = []
              for (var k = 0; k < allKeys.length; k++) {
                if (!allKeys[k].startsWith("key")) {
                  valueKeys.push(allKeys[k])
                }
              }
              for (var i = 0; i < totalArraySize; i++) {
                var result = {}
                for (var k = 0; k < valueKeys.length; k++) {
                  result[valueKeys[k]] = 0 // Math.floor(Math.random() * 100);
                }
                for (var d = 0; d < numDimensions; d++) {
                  // now add dimension keys
                  result["key" + d] = counters[d]
                }
                filledResults.push(result)
                for (var d = numDimensions - 1; d >= 0; d--) {
                  // now add dimension keys
                  counters[d] += 1
                  if (counters[d] < dimensionSizes[d]) {
                    break
                  } else {
                    counters[d] = 0
                  }
                }
              }
              for (var r = 0; r < numResults; r++) {
                var index = 0
                for (var d = 0; d < numDimensions; d++) {
                  index += results[r]["key" + d] * dimensionSums[d]
                }
                filledResults[index] = results[r]
              }
              return filledResults
            }
          } else {
            return results
          }
        }

        /* set ordering to expression */
        function order(orderExpression) {
          _orderExpression = orderExpression
          return group
        }

        /* set ordering back to natural order (i.e. by measures)*/
        function orderNatural() {
          _orderExpression = null
          return group
        }

        function all(callback) {
          if (!callback) {
            console.warn(
              "Warning: Deprecated sync method group.all(). Please use async version"
            )
          }

          // freeze bin params so they don't change out from under us
          var queryBinParams = binParams()
          if (!queryBinParams.length) {
            queryBinParams = null
          }
          var query = writeQuery(queryBinParams)
          query += " ORDER BY "
          for (var d = 0; d < dimArray.length; d++) {
            if (d > 0) query += ","
            query += "key" + d.toString()
          }

          var postProcessors = [
            function unBinResultsForAll(results) {
              if (queryBinParams) {
                const filledResults = fillBins(queryBinParams, results)
                return unBinResults(queryBinParams, filledResults)
              } else {
                return results
              }
            }
          ]

          var options = {
            eliminateNullRows: eliminateNull,
            renderSpec: null,
            postProcessors: postProcessors,
            queryId: dimensionIndex
          }

          if (callback) {
            return cache.queryAsync(query, options, callback)
          } else {
            return cache.query(query, options)
          }
        }

        function minMaxWithFilters({ min = "min_val", max = "max_val" } = {}) {
          const filters = writeFilter()
          const filterQ = filters.length ? `WHERE ${filters}` : ""
          const query = `SELECT MIN(${dimArray[0]}) as ${min}, MAX(${
            dimArray[0]
          }) as ${max} FROM ${_tablesStmt} ${filterQ}`

          var options = {
            eliminateNullRows: eliminateNull,
            postProcessors: [d => d[0]],
            renderSpec: null,
            queryId: -1
          }

          return new Promise((resolve, reject) => {
            cache.emptyCache()
            cache.queryAsync(query, options, (error, val) => {
              if (error) {
                reject(error)
              } else {
                resolve(val)
              }
            })
          })
        }

        function writeTopBottomQuery(
          k,
          offset,
          ascDescExpr,
          ignoreFilters,
          isRender
        ) {
          var queryBinParams = binParams()
          var query = writeQuery(
            queryBinParams.length ? queryBinParams : null,
            _orderExpression,
            ignoreFilters,
            !!isRender
          )

          if (!query) {
            return ""
          }

          query += " ORDER BY "
          if (_orderExpression) {
            query += _orderExpression + ascDescExpr
          } else {
            var reduceArray = reduceVars.split(",")
            var reduceSize = reduceArray.length
            for (var r = 0; r < reduceSize - 1; r++) {
              query += reduceArray[r] + ascDescExpr + ","
            }
            query += reduceArray[reduceSize - 1] + ascDescExpr
          }

          if (k != Infinity) {
            query += " LIMIT " + k
          }
          if (offset !== undefined) query += " OFFSET " + offset

          return query
        }

        function writeTopQuery(k, offset, ignoreFilters, isRender) {
          return writeTopBottomQuery(
            k,
            offset,
            " DESC",
            ignoreFilters,
            isRender
          )
        }

        function top(k, offset, renderSpec, callback, ignoreFilters) {
          if (!callback) {
            console.warn(
              "Warning: Deprecated sync method group.top(). Please use async version"
            )
          }

          // freeze bin params so they don't change out from under us
          var queryBinParams = binParams()
          if (!queryBinParams.length) {
            queryBinParams = null
          }

          var query = writeTopQuery(k, offset, ignoreFilters, !!renderSpec)

          var postProcessors = [
            function unBinResultsForTop(results) {
              if (queryBinParams) {
                return unBinResults(queryBinParams, results)
              } else {
                return results
              }
            }
          ]

          var options = {
            eliminateNullRows: eliminateNull,
            renderSpec: renderSpec,
            postProcessors: postProcessors,
            queryId: dimensionIndex
          }

          if (callback) {
            return cache.queryAsync(query, options, callback)
          } else {
            return cache.query(query, options)
          }
        }

        function topAsync(k, offset, renderSpec, ignoreFilters) {
          return new Promise((resolve, reject) => {
            top(
              k,
              offset,
              renderSpec,
              (error, result) => {
                if (error) {
                  reject(error)
                } else {
                  resolve(result)
                }
              },
              ignoreFilters
            )
          })
        }

        function writeBottomQuery(k, offset, ignoreFilters, isRender) {
          return writeTopBottomQuery(k, offset, "", ignoreFilters, isRender)
        }

        function bottom(k, offset, renderSpec, callback, ignoreFilters) {
          if (!callback) {
            console.warn(
              "Warning: Deprecated sync method group.bottom(). Please use async version"
            )
          }

          // freeze bin params so they don't change out from under us
          var queryBinParams = binParams()
          if (!queryBinParams.length) {
            queryBinParams = null
          }

          var query = writeBottomQuery(k, offset, ignoreFilters, !!renderSpec)

          var postProcessors = [
            function unBinResultsForBottom(results) {
              if (queryBinParams) {
                return unBinResults(queryBinParams, results)
              } else {
                return results
              }
            }
          ]

          var options = {
            eliminateNullRows: eliminateNull,
            renderSpec: null,
            postProcessors: postProcessors,
            queryId: dimensionIndex
          }

          if (callback) {
            return cache.queryAsync(query, options, callback)
          } else {
            return cache.query(query, options)
          }
        }

        function reduceCount(countExpression, name) {
          reduce([
            {
              expression: countExpression,
              agg_mode: "count",
              name: name || "val"
            }
          ])
          return group
        }

        function reduceSum(sumExpression, name) {
          reduce([
            { expression: sumExpression, agg_mode: "sum", name: name || "val" }
          ])
          return group
        }

        function reduceAvg(avgExpression, name) {
          reduce([
            { expression: avgExpression, agg_mode: "avg", name: name || "val" }
          ])
          return group
        }

        function reduceMin(minExpression, name) {
          reduce([
            { expression: minExpression, agg_mode: "min", name: name || "val" }
          ])
          return group
        }

        function reduceMax(maxExpression, name) {
          reduce([
            { expression: maxExpression, agg_mode: "max", name: name || "val" }
          ])
          return group
        }

        // expressions should be an array of
        // { expression, agg_mode (sql_aggregate), name, filter (optional) }
        function reduce(expressions) {
          // _reduceTableSet = {};

          if (!arguments.length) {
            return reduceSubExpressions
          }
          reduceSubExpressions = expressions
          reduceExpression = ""
          reduceVars = ""
          var numExpressions = expressions.length
          for (var e = 0; e < numExpressions; e++) {
            if (e > 0) {
              reduceExpression += ","
              reduceVars += ","
            }
            if (
              e == targetSlot &&
              targetFilter != null &&
              targetFilter != dimensionIndex &&
              filters[targetFilter] != ""
            ) {
              // this is the old way
              // reduceExpression += "AVG(CAST(" + filters[targetFilter] + " AS INT))"
              reduceExpression +=
                " AVG(CASE WHEN " +
                filters[targetFilter] +
                " THEN 1 ELSE 0 END)"
            } else {
              /*
               * if (expressions[e].expression in columnTypeMap) {
               *   _reduceTableSet[columnTypeMap[expressions[e].expression].table] =
               *     (_reduceTableSet[columnTypeMap[expressions[e].expression].table] || 0) + 1;
               *  }
               */

              var agg_mode = expressions[e].agg_mode.toUpperCase()

              if (agg_mode === "CUSTOM") {
                reduceExpression += expressions[e].expression
              } else if (agg_mode == "COUNT") {
                if (expressions[e].filter) {
                  reduceExpression +=
                    "COUNT(CASE WHEN " + expressions[e].filter + " THEN 1 END)"
                } else {
                  if (typeof expressions[e].expression !== "undefined") {
                    reduceExpression +=
                      "COUNT(" + expressions[e].expression + ")"
                  } else {
                    reduceExpression += "COUNT(*)"
                  }
                }
              } else {
                // should check for either sum, avg, min, max
                if (expressions[e].filter) {
                  reduceExpression +=
                    agg_mode +
                    "(CASE WHEN " +
                    expressions[e].filter +
                    " THEN " +
                    expressions[e].expression +
                    " END)"
                } else {
                  reduceExpression +=
                    agg_mode + "(" + expressions[e].expression + ")"
                }
              }
            }
            reduceExpression += " AS " + expressions[e].name
            reduceVars += expressions[e].name
          }
          return group
        }

        function size(ignoreFilters, callback) {
          if (!callback) {
            console.warn(
              "Warning: Deprecated sync method group.size(). Please use async version"
            )
          }
          var stateSlice = { isMultiDim, _joinStmt, _tablesStmt, dimArray }
          var queryTask = _dataConnector.query.bind(_dataConnector)
          var sizeAsync = sizeAsyncWithEffects(queryTask, writeFilter)
          var sizeSync = sizeSyncWithEffects(queryTask, writeFilter)
          if (callback) {
            sizeAsync(stateSlice, ignoreFilters, callback)
          } else {
            return sizeSync(stateSlice, ignoreFilters)
          }
        }

        /* istanbul ignore next */
        function sizeAsync(ignoreFilters) {
          return new Promise((resolve, reject) => {
            size(ignoreFilters, (error, data) => {
              if (error) {
                reject(error)
              } else {
                resolve(data)
              }
            })
          })
        }

        return reduceCount()
      }

      function dispose() {
        filters[dimensionIndex] = null
        dimensions[dimensionIndex] = null
      }

      var nonAliasedDimExpression = ""

      dimensions.push(dimensionExpression)
      for (var d = 0; d < dimArray.length; d++) {
        if (dimArray[d] in columnTypeMap) {
          dimContainsArray[d] = columnTypeMap[dimArray[d]].is_array
        } else if (dimArray[d] in compoundColumnMap) {
          dimContainsArray[d] =
            columnTypeMap[compoundColumnMap[dimArray[d]]].is_array
        } else {
          dimContainsArray[d] = false
        }
      }
      return dimension
    }

    function groupAll() {
      var group = {
        reduceCount: reduceCount,
        reduceSum: reduceSum,
        reduceAvg: reduceAvg,
        reduceMin: reduceMin,
        reduceMax: reduceMax,
        reduce: reduce,
        reduceMulti: reduce, // alias for backward compatibility
        value: value,
        valueAsync: valueAsync,
        values: values,
        valuesAsync: valuesAsync,
        getCrossfilter: function() {
          return crossfilter
        },
        getCrossfilterId: crossfilter.getId,
        getTable: crossfilter.getTable,
        getReduceExpression: function() {
          return reduceExpression
        }, // TODO for testing only
        size: size,
        sizeAsync: sizeAsync
      }
      var reduceExpression = null
      var maxCacheSize = 5
      var cache = resultCache(_dataConnector)

      function writeFilter(ignoreFilters, ignoreChartFilters) {
        var filterQuery = ""
        var validFilterCount = 0

        if (!ignoreChartFilters) {
          for (var i = 0; i < filters.length; i++) {
            if (filters[i] && filters[i] != "") {
              if (validFilterCount > 0) {
                filterQuery += " AND "
              }
              validFilterCount++
              filterQuery += filters[i]
            }
          }
        }

        if (!ignoreFilters) {
          for (var i = 0; i < globalFilters.length; i++) {
            if (globalFilters[i] && globalFilters[i] != "") {
              if (validFilterCount > 0) {
                filterQuery += " AND "
              }
              validFilterCount++
              filterQuery += globalFilters[i]
            }
          }
        }
        return isRelative(filterQuery)
          ? replaceRelative(filterQuery)
          : filterQuery
      }

      function writeQuery(ignoreFilters, ignoreChartFilters) {
        var query = "SELECT " + reduceExpression + " FROM " + _tablesStmt
        var filterQuery = writeFilter(ignoreFilters, ignoreChartFilters)
        if (filterQuery != "") {
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

        // could use alias "key" here
        return query
      }

      function reduceCount(countExpression, name) {
        if (typeof countExpression !== "undefined")
          reduceExpression =
            "COUNT(" + countExpression + ") as " + (name || "val")
        else reduceExpression = "COUNT(*) as val"
        return group
      }

      function reduceSum(sumExpression, name) {
        reduceExpression = "SUM(" + sumExpression + ") as " + (name || "val")
        return group
      }

      function reduceAvg(avgExpression, name) {
        reduceExpression = "AVG(" + avgExpression + ") as " + (name || "val")
        return group
      }

      function reduceMin(minExpression, name) {
        reduceExpression = "MIN(" + minExpression + ") as " + (name || "val")
        return group
      }

      function reduceMax(maxExpression, name) {
        reduceExpression = "MAX(" + maxExpression + ") as " + (name || "val")
        return group
      }

      function reduce(expressions) {
        //expressions should be an array of {expression, agg_mode (sql_aggregate), name}
        reduceExpression = ""
        var numExpressions = expressions.length
        for (var e = 0; e < numExpressions; e++) {
          if (e > 0) {
            reduceExpression += ","
          }
          var agg_mode = expressions[e].agg_mode.toUpperCase()

          if (agg_mode === "CUSTOM") {
            reduceExpression += expressions[e].expression
          } else if (agg_mode == "COUNT") {
            if (typeof expressions[e].expression !== "undefined") {
              reduceExpression += "COUNT(" + expressions[e].expression + ")"
            } else {
              reduceExpression += "COUNT(*)"
            }
          } else {
            // should check for either sum, avg, min, max
            reduceExpression += agg_mode + "(" + expressions[e].expression + ")"
          }
          reduceExpression += " AS " + expressions[e].name
        }
        return group
      }

      function value(ignoreFilters, ignoreChartFilters, callback) {
        if (!callback) {
          console.warn(
            "Warning: Deprecated sync method groupAll.value(). Please use async version"
          )
        }
        var query = writeQuery(ignoreFilters, ignoreChartFilters)
        var options = {
          eliminateNullRows: false,
          renderSpec: null,
          postProcessors: [
            function(d) {
              return d[0].val
            }
          ],
          queryId: -1
        }

        if (callback) {
          return cache.queryAsync(query, options, callback)
        } else {
          return cache.query(query, options)
        }
      }

      function valueAsync(ignoreFilters = false, ignoreChartFilters = false) {
        return new Promise((resolve, reject) => {
          value(ignoreFilters, ignoreChartFilters, (error, result) => {
            if (error) {
              reject(error)
            } else {
              resolve(result)
            }
          })
        })
      }

      function values(ignoreFilters, ignoreChartFilters, callback) {
        if (!callback) {
          console.warn(
            "Warning: Deprecated sync method groupAll.values(). Please use async version"
          )
        }
        var query = writeQuery(ignoreFilters, ignoreChartFilters)
        var options = {
          eliminateNullRows: false,
          renderSpec: null,
          postProcessors: [
            function(d) {
              return d[0]
            }
          ],
          queryId: -1
        }
        if (callback) {
          return cache.queryAsync(query, options, callback)
        } else {
          return cache.query(query, options)
        }
      }

      function valuesAsync(ignoreFilters = false, ignoreChartFilters = false) {
        return new Promise((resolve, reject) => {
          values(ignoreFilters, ignoreChartFilters, (error, data) => {
            if (error) {
              reject(error)
            } else {
              resolve(data)
            }
          })
        })
      }

      return reduceCount()
    }

    // Returns the number of records in this crossfilter, irrespective of any filters.
    function size(callback) {
      if (!callback) {
        console.warn(
          "Warning: Deprecated sync method groupAll.size(). Please use async version"
        )
      }
      var query = "SELECT COUNT(*) as n FROM " + _tablesStmt

      if (_joinStmt !== null) {
        query += " WHERE " + _joinStmt
      }

      var options = {
        eliminateNullRows: false,
        renderSpec: null,
        postProcessors: [
          function(d) {
            return d[0].n
          }
        ]
      }
      if (callback) {
        return cache.queryAsync(query, options, callback)
      } else {
        return cache.query(query, options)
      }
    }

    function sizeAsync() {
      return new Promise((resolve, reject) => {
        size((error, data) => {
          if (error) {
            reject(error)
          } else {
            resolve(data)
          }
        })
      })
    }

    return arguments.length >= 2
      ? setDataAsync(arguments[0], arguments[1], arguments[2]) // dataConnector, dataTable
      : crossfilter
  }
})((typeof exports !== "undefined" && exports) || this)
