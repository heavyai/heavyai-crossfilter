// TODO everything should be async

// polyfill for browser compat
Array.prototype.includes = Array.prototype.includes || function (searchElement, fromIndex) {
  return this.slice(fromIndex || 0).indexOf(searchElement) >= 0;
};

function filterNullMeasures(filterStatement, measures) {
  var measureNames = measures.filter(notEmptyNotStarNotComposite).map(toProp("expression"));
  var maybeParseParameters = flatten(measureNames.map(parseParensIfExist));
  var nullColumnsFilter = maybeParseParameters.map(isNotNull).join(" AND ");
  var newfilterStatement = maybeAnd(filterStatement, nullColumnsFilter);
  return newfilterStatement;
}
function toProp(propName) { return function (item) { return item[propName]; }; }
function isNotNull(columnName) { return columnName + " IS NOT NULL"; }
function notEmptyNotStarNotComposite(item) {
  return notEmpty(item.expression) && item.expression !== "*" && !item.isComposite;
}

function parseParensIfExist(measureValue) {
  // slightly hacky regex, but goes down for 4 levels deep in terms of nesting ().
  var checkParens = /\(([^()]*|\(([^()]*|\(([^()]*|\([^()]*\))*\))*\))*\)/g;
  var thereIsParens = checkParens.test(measureValue);

  if (thereIsParens) {
    var parsedParens = measureValue.match(checkParens);
    return parsedParens.map(function (str) {
      return str.slice(1, -1);
    });
  } else {
    return [measureValue];
  }
}
function flatten(arr) {
  return arr.reduce(function (flat, toFlatten) {
    return flat.concat(Array.isArray(toFlatten) ? flatten(toFlatten) : toFlatten);
  }, []);
}

function notEmpty(item) {
  switch (typeof item) {
    case "undefined": return false;
    case "boolean": return true;
    case "number": return true;
    case "symbol": return true;
    case "function": return true;
    case "string": return item.length > 0;

    // null, array, object, date
    case "object": return item !== null && (typeof item.getDay === "function" || Object.keys(item).length > 0); // jscs:ignore maximumLineLength
  }
}

function maybeAnd(clause1, clause2) {
  var joiningWord = clause1 === "" || clause2 === "" ? "" : " AND ";
  return clause1 + joiningWord + clause2;
}

function _mapColumnsToNameAndType(columns) {
  return Object.keys(columns).map(function (key) {
    var col = columns[key];
    return { rawColumn: key, column: col.column, type: col.type };
  });
}

function _findIndexOfColumn(columns, targetColumn) {
  return columns.reduce(function (colIndex, col, i) {
    var containsField = col.rawColumn === targetColumn || col.column === targetColumn;
    if (colIndex === -1 && containsField) { colIndex = i; }
    return colIndex;
  }, -1);
}

function _isDateField(field) { return field.type === "DATE"; }

(function (exports) {
  crossfilter.version = "1.3.11";
  exports.resultCache = resultCache;
  exports.crossfilter = crossfilter;
  exports.filterNullMeasures = filterNullMeasures;
  exports.notEmpty = notEmpty;
  exports.parseParensIfExist = parseParensIfExist;

  function resultCache(con) {
    var resultCache = {
      query: query,
      queryAsync: queryAsync,
      emptyCache: emptyCache,
      setMaxCacheSize: function (size) {
        maxCacheSize = size;
      },
      setDataConnector: function (con) {
        _dataConnector = con;
      },
      peekAtCache: function () { return cache; }, // TODO test only
      getMaxCacheSize: function () {return maxCacheSize; }, // TODO test only
      getDataConnector: function () {return _dataConnector; }, // TODO test only
    };

    var maxCacheSize = 10; // TODO should be top-level constant or init param
    var cache = {};
    var cacheCounter = 0;
    var _dataConnector = null; // TODO con not used elsewhere

    function evictOldestCacheEntry() {
      var oldestQuery = null;
      var lowestCounter = Number.MAX_SAFE_INTEGER;
      for (var key in cache) {
        if (cache[key].time < lowestCounter) {
          oldestQuery = key;
          lowestCounter = cache[key].time;
        }
      }
      delete cache[oldestQuery];
    }

    function emptyCache() {
      cache = {};
      return resultCache;
    }

    function queryAsync(query, options, callbacks) {
      var eliminateNullRows = false;
      var renderSpec = null;
      var postProcessors = null;
      var queryId = null;
      if (options) {
        eliminateNullRows = options.eliminateNullRows ? options.eliminateNullRows : false;
        renderSpec = options.renderSpec ? options.renderSpec : null;
        postProcessors = options.postProcessors ? options.postProcessors : null;
        queryId = options.queryId ? options.queryId : null;
      }

      var numKeys = Object.keys(cache).length;

      if (!renderSpec) {
        if (query in cache) {
          cache[query].time = cacheCounter++;

          // change selector to null as it should already be in cache
          // no postProcessors, shouldCache: true
          asyncCallback(query, null, !renderSpec, cache[query].data, callbacks);
          return;
        }
        if (numKeys >= maxCacheSize) { // should never be gt
          evictOldestCacheEntry(); // TODO only reachable if query not in cache
        }
      }

      // TODO needs to bind callbacks
      callbacks.push(asyncCallback.bind(this, query, postProcessors, !renderSpec));
      var conQueryOptions = {
        columnarResults: true,
        eliminateNullRows: eliminateNullRows,
        renderSpec: renderSpec,
        queryId: queryId,
      };
      return _dataConnector.query(query, conQueryOptions, callbacks);
    }

    function asyncCallback(query, postProcessors, shouldCache, result, callbacks) {
      callbacks = callbacks || []; // TODO need to typecheck callbacks
      if (!shouldCache) {
        if (!postProcessors) {
          callbacks.pop()(result, callbacks);
        } else {
          var data = result;
          for (var s = 0; s < postProcessors.length; s++) {
            data = postProcessors[s](result);
          }
          callbacks.pop()(data, callbacks);
        }
      } else {
        if (!postProcessors) {
          cache[query] = { time: cacheCounter++, data: result };
        } else {
          var data = result;
          for (var s = 0; s < postProcessors.length; s++) {
            data = postProcessors[s](data);
          }
          cache[query] = { time: cacheCounter++, data: data };
        }

        // callbacks.pop()(cache[query].data, callbacks); // TODO should be this, not forEach
        callbacks.forEach(function (cb) { cb(cache[query].data, callbacks); });
      }
    }

    function query(query, options) {
      var eliminateNullRows = false;
      var renderSpec = null;
      var postProcessors = null;
      var queryId = null;
      if (options) {
        eliminateNullRows = options.eliminateNullRows ? options.eliminateNullRows : false;
        renderSpec = options.renderSpec ? options.renderSpec : null;
        postProcessors = options.postProcessors ? options.postProcessors : null;
        queryId = options.queryId ? options.queryId : null;
      }

      var numKeys = Object.keys(cache).length;

      if (!renderSpec) {
        if (query in cache) {
          cache[query].time = cacheCounter++;
          return cache[query].data;
        }
        if (numKeys >= maxCacheSize) { // should never be gt
          evictOldestCacheEntry();
        }
      }
      var data = null;
      var conQueryOptions = {
        columnarResults: true,
        eliminateNullRows: eliminateNullRows,
        renderSpec: renderSpec,
        queryId: queryId,
      };
      if (!postProcessors) {
        data = _dataConnector.query(query, conQueryOptions);
        if (!renderSpec) {
          cache[query] = { time: cacheCounter++, data: data };
        }
      } else {
        data = _dataConnector.query(query, conQueryOptions);
        for (var s = 0; s < postProcessors.length; s++) {
          data = postProcessors[s](data);
        }
        if (!renderSpec) {
          cache[query] = { time: cacheCounter++, data: data };
        }
      }
      return data;
    }

    _dataConnector = con; // TODO unnecessary
    return resultCache;
  }

  function crossfilter() {

    var crossfilter = {
      type: "crossfilter",
      setData: setData,
      filter: filter,
      getColumns: getColumns,
      dimension: dimension,
      groupAll: groupAll,
      size: size,
      getFilter: function () { return filters; },
      getFilterString: getFilterString,
      getDimensions: function () { return dimensions; },
      getTable: function () { return _dataTables; },
      peekAtCache: function () { return cache.peekAtCache(); }, // TODO test only
    };

    var _dataTables = null;
    var _joinAttrMap = {};
    var _joinStmt = null;
    var _tablesStmt = null;
    var filters = [];
    var targetFilter = null;
    var columnTypeMap = null;
    var compoundColumnMap = null;
    var _dataConnector = null;
    var dimensions = [];
    var globalFilters = [];
    var cache = null;

    var TYPES = {
      "undefined": "undefined",
      "number": "number",
      "boolean": "boolean",
      "string": "string",
      "[object Function]": "function",
      "[object RegExp]": "regexp",
      "[object Array]": "array",
      "[object Date]": "date",
      "[object Error]": "error",
    };

    var TOSTRING = Object.prototype.toString;

    function type(o) {
      return TYPES[typeof o] || TYPES[TOSTRING.call(o)] || (o ? "object" : "null");
    }

    function setData(dataConnector, dataTables, joinAttrs) {
      /* joinAttrs should be an array of objects with keys
       * table1, table2, attr1, attr2
       */

      _dataConnector = dataConnector;
      cache = resultCache(_dataConnector);
      _dataTables = dataTables;
      if (!Array.isArray(_dataTables)) {
        _dataTables = [_dataTables];
      }
      _tablesStmt = "";
      _dataTables.forEach(function (table, i) {
        if (i > 0) {
          _tablesStmt += ",";
        }
        _tablesStmt += table;
      });
      _joinStmt = null;
      if (typeof joinAttrs !== "undefined") {
        _joinAttrMap = {};
        _joinStmt = "";
        joinAttrs.forEach(function (join, i) {
          var joinKey = join.table1 < join.table2 ?
            join.table1 + "." + join.table2 : join.table2 + "." + join.table1;
          var tableJoinStmt = join.table1 + "." + join.attr1 + " = "
            + join.table2 + "." + join.attr2;
          if (i > 0) {
            _joinStmt += " AND ";
          }
          _joinStmt += tableJoinStmt;
          _joinAttrMap[joinKey] = tableJoinStmt;
        });
      }
      var columnNameCountMap = {};
      columnTypeMap = {};
      compoundColumnMap = {};
      _dataTables.forEach(function (table) {
        var columnsArray = _dataConnector.getFields(table);

        columnsArray.forEach(function (element) {
          var compoundName = table + "." + element.name;
          columnTypeMap[compoundName] = {
            table: table,
            column: element.name,
            type: element.type,
            is_array: element.is_array,
            is_dict: element.is_dict,
            name_is_ambiguous: false,
          };
          columnNameCountMap[element.name] = columnNameCountMap[element.name] === undefined ?
            1 : columnNameCountMap[element.name] + 1;
        });
      });
      for (var key in columnTypeMap) {
        if (columnNameCountMap[columnTypeMap[key].column] > 1) {
          columnTypeMap[key].name_is_ambiguous = true;
        } else {
          compoundColumnMap[columnTypeMap[key].column] = key;
        }
      }
      return crossfilter;
    }

    function getColumns() {
      return columnTypeMap;
    }

    function getFilterString() {
      var filterString = "";
      var firstElem = true;
      filters.forEach(function (value) {
        if (value != null && value != "") {
          if (!firstElem) {
            filterString += " AND ";
          }
          firstElem = false;
          filterString += value;
        }
      });
      return filterString;
    }

    function filter() {
      var filter = {
        filter: filter,
        filterAll: filterAll,
        getFilter: getFilter,
        toggleTarget: toggleTarget,
        getTargetFilter: function () { return targetFilter; }, // TODO for test only
      };

      var filterIndex = filters.length;
      filters.push("");

      function toggleTarget() {
        if (targetFilter == filterIndex) {
          targetFilter = null;
        } else {
          targetFilter = filterIndex;
        }
        return filter;
      }

      function getFilter() {
        return filters[filterIndex];
      }

      function filter(filterExpr) {
        if (filterExpr == undefined || filterExpr ==  null) {
          filterAll();
        } else {
          filters[filterIndex] = filterExpr;
        }
        return filter;
      }

      function filterAll() {
        filters[filterIndex] = "";
        return filter;
      }

      return filter;
    }

    function dimension(expression) {
      var dimension = {
        type: "dimension",
        order: order,
        orderNatural: orderNatural,
        selfFilter: selfFilter,
        filter: filter,
        filterExact: filterExact,
        filterRange: filterRange,
        filterAll: filterAll,
        filterMulti: filterMulti,
        filterLike: filterLike,
        filterILike: filterILike,
        getFilter: getFilter,
        getFilterString: getFilterString,
        projectOn: projectOn,
        getProjectOn: function () { return projectExpressions; },
        projectOnAllDimensions: projectOnAllDimensions,
        getResultSet: function () { return resultSet; },
        samplingRatio: samplingRatio,
        top: top,
        topAsync: top,
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
        value: function () { return dimArray; },

        // makes filter conjunctive
        setDrillDownFilter: function (v) {
          drillDownFilter = v;
          return dimension;
        },

        getSamplingRatio: function () { return samplingRatio; }, // TODO for tests only
      };
      var filterVal = null;
      var _allowTargeted = true;
      var _selfFilter = null;
      var dimensionIndex = filters.length;
      var dimensionGroups = [];
      var _orderExpression = null;
      filters.push("");
      var projectExpressions = [];
      var projectOnAllDimensionsFlag = false;
      var binBounds = null; // for binning
      var rangeFilters = [];
      var resultSet = null;
      var dimContainsArray = [];

      // option for array columns
      // - means observe own filter and use conjunctive instead of disjunctive between sub-filters
      var drillDownFilter = false;
      var cache = resultCache(_dataConnector);
      var dimensionExpression = null;
      var samplingRatio = null;

      var expression = Array.isArray(expression) ? expression : [expression];
      var multiDim = expression.length > 1;

      var columns = _mapColumnsToNameAndType(crossfilter.getColumns());
      var dimArray = expression.map(function (field) {
        var indexOfColumn = _findIndexOfColumn(columns, field);
        var isDate = indexOfColumn > -1 && _isDateField(columns[indexOfColumn]);
        if (isDate) {
          field = "CAST(" + field + " AS TIMESTAMP(0))";
        }
        return field;
      });

      dimensionExpression = dimArray.includes(null) ? null : dimArray.join(", ");

      function order(orderExpression) {
        _orderExpression = orderExpression;
        return dimension;
      }

      function orderNatural() {
        _orderExpression = null;
        return dimension;
      }

      function selfFilter(_) {
        if (!arguments.length)
          return _selfFilter;
        _selfFilter = _;
        return dimension;
      }

      function allowTargeted(allowTargeted) {
        if (!arguments.length)
          return _allowTargeted;

        // Code below may need to be moved into group
        //if (!allowTargeted && _allowTargeted && targetFilter !== null) {
        //  //jquery(group).trigger("untargeted");
        //  reduceMulti(reduceSubExpressions);
        //  lastTargetFilter = null;
        //}
        //else if (allowTargeted && !_allowTargeted && targetFilter !== null) {
        //  //jquery(group).trigger("targeted", [filters[targetFilter]]);
        //  reduceMulti(reduceSubExpressions);
        //  lastTargetFilter = targetFilter;
        //}
        _allowTargeted = allowTargeted;
        return dimension;
      }

      function toggleTarget() {
        if (targetFilter == dimensionIndex) { // TODO duplicates isTargeting
          targetFilter = null; // TODO duplicates removeTarget
        } else {
          targetFilter = dimensionIndex;
        }
      }

      function removeTarget() {
        if (targetFilter == dimensionIndex) {
          targetFilter = null;
        }
      }

      function isTargeting() {
        return targetFilter == dimensionIndex;
      }

      function projectOn(expressions) {
        projectExpressions = expressions;
        return dimension;
      }

      function projectOnAllDimensions(flag) {
        projectOnAllDimensionsFlag = flag;
        return dimension;
      }

      function getFilter() {
        return filterVal;
      }

      function getFilterString() {
        return filters[dimensionIndex];
      }

      function filter(range, append, resetRange, jqueryStub) {
        append = typeof append !== "undefined" ? append : false;
        return range == null
          ? filterAll(undefined, jqueryStub) : Array.isArray(range) && !multiDim
          ? filterRange(range, append, resetRange) : typeof range === "function"
          ? filterFunction(range, append) // TODO filterFunction not defined
          : filterExact(range, append);
      }

      function formatFilterValue(value, wrapInQuotes) {
        var valueType = type(value);
        if (valueType == "string") {
          var escapedValue = value
            .replace(/\'/g, "''")
            .replace(/\%/g, "\\%")
            .replace(/\_/g, "\\_");
          return wrapInQuotes ? "'" + escapedValue + "'" : escapedValue;
        } else if (valueType == "date") {
          return "TIMESTAMP(0) '" + value.toISOString().slice(0, 19).replace("T", " ") + "'";
        } else {
          return value;
        }
      }

      function filterExact(value, append) {
        var isArray = Array.isArray(value);
        if (!isArray) {
          value = [value];
        }
        var subExpression = "";
        for (var e = 0; e < value.length; e++) {
          if (e > 0) {
            subExpression += " AND ";
          }
          var typedValue = formatFilterValue(value[e], true);
          if (dimContainsArray[e]) {
            subExpression += typedValue + " = ANY " + dimArray[e];
          } else {
            subExpression += dimArray[e] + " = " + typedValue;
          }
        }

        append = typeof append !== "undefined" ? append : false;
        if (append) {
          filters[dimensionIndex] += subExpression;
        } else {
          filters[dimensionIndex] = subExpression;
        }
        return dimension;
      }

      function filterLike(value, append) {
        append = typeof append !== "undefined" ? append : false;
        var escaped = formatFilterValue(value, false);
        if (append) {
          filters[dimensionIndex] += dimensionExpression + " like '%" + escaped + "%'";
        } else {
          filters[dimensionIndex] = dimensionExpression + " like '%" + escaped + "%'";
        }
      } // TODO should it return dimension?

      function filterILike(value) {
        append = typeof append !== "undefined" ? append : false; // TODO unnecessary
        var escaped = formatFilterValue(value, false);
        if (append) { // TODO always false; unreachable code
          filters[dimensionIndex] += dimensionExpression + " ilike '%" + escaped + "%'";
        } else {
          filters[dimensionIndex] = dimensionExpression + " ilike '%" + escaped + "%'";
        }
      } // TODO should it return dimension?

      function filterRange(range, append, resetRange) {
        var isArray = Array.isArray(range[0]); // TODO semi-risky index
        if (!isArray) {
          range = [range];
        }
        filterVal = range;
        var subExpression = "";

        for (var e = 0; e < range.length; e++) {
          if (resetRange == true) {
            rangeFilters[e] = range[e];
          }
          if (e > 0) {
            subExpression += " AND ";
          }

          var typedRange = [
            formatFilterValue(range[e][0], true),
            formatFilterValue(range[e][1], true),
          ];
          subExpression += dimArray[e] + " >= " + typedRange[0] + " AND "
            + dimArray[e] + " < " + typedRange[1];
        }

        append = typeof append !== "undefined" ? append : false;
        if (append) {
          filters[dimensionIndex] += "(" + subExpression + ")";
        } else {
          filters[dimensionIndex] = "(" + subExpression + ")";
        }
        return dimension;
      }

      function filterMulti(filterArray, resetRangeIn, jqueryStub) {

        // applying or with multiple filters"
        var jquery = typeof $ === "undefined" ? jqueryStub : $;

        //filterVal = filterArray;
        var filterWasNull = filters[dimensionIndex] == null || filters[dimensionIndex] == "";
        var resetRange = false;
        if (resetRangeIn !== undefined) {
          resetRange = resetRangeIn;
          if (resetRange == true) {
            jquery(dimension).trigger("reranged"); // TODO jQuery dependency
          }
        }

        var lastFilterIndex = filterArray.length - 1;
        filters[dimensionIndex] = "(";

        for (var i = 0; i <= lastFilterIndex; i++) {
          var curFilter = filterArray[i];
          filter(curFilter, true, resetRange);
          if (i != lastFilterIndex && drillDownFilter) {

            // a bit weird to have this in filterMulti -
            // but good for top level functions not to know whether this is a drilldownfilter or not
            filters[dimensionIndex] += " AND ";
          } else if (i != lastFilterIndex && !drillDownFilter) {
            filters[dimensionIndex] += " OR ";
          }
        }
        filters[dimensionIndex] += ")";

        // TODO can never be true due to previous line assignment
        var filterNowNull = filters[dimensionIndex] == null || filters[dimensionIndex] == "";
        if (filterWasNull && !filterNowNull) {
          jquery(this).trigger("filter-on"); // TODO jQuery dependency
        } else if (!filterWasNull && filterNowNull) {
          jquery(this).trigger("filter-clear"); // TODO jQuery dependency; unreachable?
        }
        return dimension;
      }

      function filterAll(softFilterClear, jqueryStub) {
        var jquery = typeof $ === "undefined" ? jqueryStub : $;

        if (softFilterClear == undefined || softFilterClear == false) {
          jquery(this).trigger("filter-clear");
          rangeFilters = [];
        }
        filterVal = null;
        filters[dimensionIndex] = "";
        return dimension;
      }

      // Returns the top K selected records based on this dimension"s order.
      // Note: observes this dimension"s filter, unlike group and groupAll.
      function writeQuery(hasRenderSpec) {
        var projList = "";
        if (projectOnAllDimensionsFlag) {
          var dimensions = crossfilter.getDimensions();
          var nonNullDimensions = [];
          for (var d = 0; d < dimensions.length; d++) {

            // other conditions:
            // && dimensions[d] in columnTypeMap && !columnTypeMap[dimensions[d]].is_array
            if (dimensions[d] !== null && dimensions[d] !== "") {
              nonNullDimensions.push(dimensions[d]);
            }
          }
          nonNullDimensions = nonNullDimensions.concat(projectExpressions);
          var dimSet = {};

          // now make set of unique non null dimensions
          for (var d = 0; d < nonNullDimensions.length; d++) {
            if (!(nonNullDimensions[d] in dimSet)) {
              dimSet[nonNullDimensions[d]] = null;
            }
          }
          nonNullDimensions = [];
          for (key in dimSet) {
            nonNullDimensions.push(key);
          }
          projList = nonNullDimensions.join(",");
        } else {
          projList = projectExpressions.join(",");
        }

        // stops query from happening if variables do not exist in chart
        if (projList === "") {
          return;
        }

        if (hasRenderSpec) {
          projList += "," + _dataTables[0] + ".rowid";
        }

        var query = "SELECT " + projList + " FROM " + _tablesStmt;
        var filterQuery = "";
        var nonNullFilterCount = 0;

        // we observe this dimensions filter
        for (var i = 0; i < filters.length; i++) {
          if (filters[i] && filters[i] != "") {
            if (nonNullFilterCount > 0) {
              filterQuery += " AND ";
            }
            nonNullFilterCount++;
            filterQuery += filters[i];
          }
        }
        if (_selfFilter) {
          if (filterQuery !== "") {
            filterQuery += " AND " + _selfFilter;
          } else {
            filterQuery = _selfFilter;
          }
        }
        if (filterQuery !== "") {
          query += " WHERE " + filterQuery;
        }
        if (samplingRatio !== null && samplingRatio < 1.0) {
          if (filterQuery) {
            query += " AND ";
          } else {
            query += " WHERE ";
          }

          // TODO magic numbers
          var threshold = Math.floor(4294967296  * samplingRatio);
          query += " MOD(" + _dataTables[0] + ".rowid * 265445761, 4294967296) < " + threshold;
        }
        if (_joinStmt !== null) {
          if (filterQuery === "" && (samplingRatio === null || samplingRatio >= 1.0)) {
            query += " WHERE ";
          } else {
            query += " AND ";
          }
          query += _joinStmt;
        }
        return query;
      }

      function samplingRatio(ratio) {
        if (!ratio)
          samplingRatio = null;
        samplingRatio = ratio; // TODO always overwrites; typo?
        return dimension;
      }

      function top(k, offset, renderSpec, callbacks) {
        var query = writeQuery(!!renderSpec);
        if (query == null) {
          return {};
        }
        if (_orderExpression) { // overrides any other ordering based on dimension
          query += " ORDER BY " + _orderExpression + " DESC";
        } else if (dimensionExpression)  {
          query += " ORDER BY " + dimensionExpression + " DESC";
        }

        if (k !== Infinity) {
          query += " LIMIT " + k;
        }
        if (offset !== undefined) {
          query += " OFFSET " + offset;
        }

        var async = !!callbacks;
        var options = {
          eliminateNullRows: false,
          renderSpec: renderSpec,
          postProcessors: null,
          queryId: dimensionIndex,
        };

        if (!async) {
          resultSet = cache.query(query, options);
          return resultSet;
        } else {
          if (!renderSpec) {
            callbacks.push(resultSetCallback.bind(this)); // need this?
          }
          return cache.queryAsync(query, options, callbacks);
        }
      }

      function bottom(k, offset, renderSpec, callbacks) {
        var query = writeQuery(!!renderSpec);
        if (query == null) {
          return {};
        }
        if (_orderExpression) { // overrides any other ordering based on dimension
          query += " ORDER BY " + _orderExpression + " ASC";
        } else if (dimensionExpression)  {
          query += " ORDER BY " + dimensionExpression + "ASC";
        }
        if (k !== Infinity) {
          query += " LIMIT " + k;
        }
        if (offset !== undefined) {
          query += " OFFSET " + offset;
        }

        var async = !!callbacks;
        var options = {
          eliminateNullRows: false,
          renderSpec: renderSpec,
          postProcessors: null,
          queryId: dimensionIndex,
        };

        if (!async) {
          resultSet = cache.query(query, options);
          return resultSet;
        } else {
          if (!renderSpec) {
            callbacks.push(resultSetCallback.bind(this)); // need this?
          }
          return cache.queryAsync(query, options, callbacks);
        }
      }

      function resultSetCallback(results, callbacks) {
        resultSet = results;
        callbacks.pop()(results, callbacks);
      }

      function group() {
        var group = {
          order: order,
          orderNatural: orderNatural,
          top: top,
          topAsync: top, //deprecated
          bottom: bottom,
          bottomAsync: bottom, //deprecated
          all: all,
          allAsync: all, // deprecated
          binParams: binParams,
          setBinParams: binParams,
          numBins: numBins,
          truncDate: truncDate,
          reduceCount: reduceCount,
          reduceSum: reduceSum,
          reduceAvg: reduceAvg,
          reduceMin: reduceMin,
          reduceMax: reduceMax,
          reduce: reduce,
          reduceMulti: reduce,
          setBoundByFilter: setBoundByFilter,
          setTargetSlot: function (s) { targetSlot = s; }, // TODO should it return group?
          getTargetSlot: function () { return targetSlot; },
          having: function () { return group; }, // TODO seems unused
          size: size,
          setEliminateNull: function (v) {
            eliminateNull = v;
            return group;
          },
          getEliminateNull: function () { return eliminateNull; }, // TODO test only

          actualTimeBin: function (dimId) {
            var queryBinParams = Array.isArray(_binParams) ? [].concat(_binParams) : [];
            if (!queryBinParams.length) {
              queryBinParams = null;
            }
            var dimTimeBin = null;
            if (queryBinParams !== null) {
              if (queryBinParams[dimId].timeBin === "auto")  {
                for (var d = 0; d < dimArray.length; d++) {
                  var binBounds = boundByFilter && rangeFilters.length > 0 ?
                    rangeFilters[d] : queryBinParams[d].binBounds;

                  dimTimeBin = getTimeBinParams(
                    [binBounds[0].getTime(), binBounds[1].getTime()],
                    queryBinParams[d].numBins
                  );
                }
              } else {
                dimTimeBin = queryBinParams[dimId].timeBin;
              }
            }
            return dimTimeBin;
          },
          writeFilter: writeFilter,
          getReduceExpression: function () { return reduceExpression; }, // TODO for testing only
        };
        var reduceExpression = null;  // count will become default
        var reduceSubExpressions = null;
        var reduceVars = null;
        var _binParams = null;
        /*
        var binCount = null;
        var binBounds = null;
        */
        var boundByFilter = false;
        var dateTruncLevel = null;
        var cache = resultCache(_dataConnector);
        var lastTargetFilter = null;
        var targetSlot = 0;
        var timeParams = null;
        var _fillMissingBins = false; // true;
        var eliminateNull = true;
        var _orderExpression = null;
        var _reduceTableSet = {};

        dimensionGroups.push(group);

        function writeFilter(queryBinParams) {
          var filterQuery = "";
          var nonNullFilterCount = 0;

          // we do not observe this dimensions filter
          for (var i = 0; i < filters.length; i++) {
            if ((i != dimensionIndex || drillDownFilter == true)
                && (!_allowTargeted || i != targetFilter)
                && (filters[i] && filters[i].length > 0)) {

              // filterQuery != "" is hack as notNullFilterCount was being incremented
              if (nonNullFilterCount > 0 && filterQuery != "") {
                filterQuery += " AND ";
              }
              nonNullFilterCount++;
              filterQuery += filters[i];
            } else if (i == dimensionIndex && queryBinParams != null) {
              var tempBinFilters = "";
              if (nonNullFilterCount > 0) {
                tempBinFilters += " AND ";
              }
              nonNullFilterCount++;
              var hasBinFilter = false;
              for (var d = 0; d < dimArray.length; d++) {
                if (queryBinParams[d] !== null) {
                  var queryBounds = queryBinParams[d].binBounds;
                  if (boundByFilter == true && rangeFilters.length > 0) {
                    queryBounds = rangeFilters[d];
                  }

                  // @todo fix - allow for interspersed nulls
                  if (d > 0 && hasBinFilter) { // TODO hasBinFilter always false; unreachable
                    tempBinFilters += " AND ";
                  }
                  hasBinFilter = true;

                  tempBinFilters += "(" + dimArray[d] +  " >= " +
                    formatFilterValue(queryBounds[0], true) + " AND " +
                    dimArray[d] + " < " + formatFilterValue(queryBounds[1], true) + ")";
                }
              }
              if (hasBinFilter) {
                filterQuery += tempBinFilters;
              }
            }
          }
          if (_selfFilter && filterQuery !== "") {
            filterQuery += " AND " + _selfFilter;
          } else if (_selfFilter && filterQuery == "") {
            filterQuery = _selfFilter;
          }
          filterQuery = filterNullMeasures(filterQuery, reduceSubExpressions);
          return filterQuery;
        }

        function getBinnedDimExpression(expression, binBounds, numBins, timeBin) { // jscs:ignore maximumLineLength
          var isDate = type(binBounds[0]) == "date";
          if (isDate) {
            if (timeBin) {
              var dimTimeBin = null;
              if (timeBin === "auto") {
                dimTimeBin = getTimeBinParams(
                  [binBounds[0].getTime(), binBounds[1].getTime()],
                  numBins
                ); // work okay with async?
              } else {
                dimTimeBin = timeBin;
              }
              var binnedExpression = "date_trunc(" + dimTimeBin + ", " + expression + ")";
              return binnedExpression;
            } else {
              var dimExpr = "extract(epoch from " + expression + ")";

              // as javscript epoch is in ms
              var filterRange = (binBounds[1].getTime() - binBounds[0].getTime()) * 0.001;

              // truncate to 9 digits to keep precision on backend
              var binsPerUnit = (numBins / filterRange).toFixed(9);
              var lowerBoundsUTC = binBounds[0].getTime() / 1000;
              var binnedExpression = "cast(" +
                "(" + dimExpr + " - " + lowerBoundsUTC + ") *" + binsPerUnit + " as int)";
              return binnedExpression;
            }
          } else {
            var filterRange = binBounds[1] - binBounds[0];

            // truncate to 9 digits to keep precision on backend
            var binsPerUnit = (numBins / filterRange).toFixed(9);
            var binnedExpression = "cast(" +
              "(" + expression + " - " + binBounds[0] + ") *" + binsPerUnit + " as int)";
            return binnedExpression;
          }
        }

        function getTimeBinParams(timeBounds, maxNumBins) {
          var epochTimeBounds = [(timeBounds[0] * 0.001), (timeBounds[1] * 0.001)];
          var timeRange = epochTimeBounds[1] - epochTimeBounds[0]; // in seconds
          var timeSpans = [
            { label: "second", numSeconds: 1 },
            { label: "minute", numSeconds: 60 },
            { label: "hour", numSeconds: 3600 },
            { label: "day", numSeconds: 86400 },
            { label: "week", numSeconds: 604800 },
            { label: "month", numSeconds: 2592000 },
            { label: "quarter", numSeconds: 10368000 },
            { label: "year", numSeconds: 31536000 },
            { label: "decade", numSeconds: 315360000 },
          ];
          for (var s = 0; s < timeSpans.length; s++) {
            if (timeRange / timeSpans[s].numSeconds < maxNumBins) {
              return timeSpans[s].label;
            }
          }
          return "century"; // default;
        }

        function writeQuery(queryBinParams, sortByValue) {
          var query = null;
          if (reduceSubExpressions
              && (_allowTargeted && (targetFilter !== null || targetFilter !== lastTargetFilter))) {
            if (targetFilter !== null
                && filters[targetFilter] !== ""
                && targetFilter !== dimensionIndex) {
              $(group).trigger("targeted", [filters[targetFilter]]);
            } else {
              $(group).trigger("untargeted");
            }
            reduce(reduceSubExpressions);
            lastTargetFilter = targetFilter;
          }

          //var tableSet = {};
          // first clone _reduceTableSet
          //for (key in _reduceTableSet)
          //  tableSet[key] = _reduceTableSet[key];
          query = "SELECT ";
          for (var d = 0; d < dimArray.length; d++) {
            // tableSet[columnTypeMap[dimArray[d]].table] =
            //   (tableSet[columnTypeMap[dimArray[d]].table] || 0) + 1;
            if (queryBinParams !== null
              && typeof queryBinParams[d] !== "undefined"
              && queryBinParams[d] !== null) {
              var binBounds = boundByFilter
                && typeof rangeFilters[d] !== "undefined"
                && rangeFilters[d] !== null ?
                  rangeFilters[d] : queryBinParams[d].binBounds;
              var binnedExpression = getBinnedDimExpression(
                dimArray[d],
                binBounds,
                queryBinParams[d].numBins,
                _binParams[d].timeBin
              );
              query += binnedExpression + " as key" + d.toString() + ",";
            } else if (dimContainsArray[d]) {
              query += "UNNEST(" + dimArray[d] + ")" + " as key" + d.toString() + ",";
            } else if (_binParams && _binParams[d] && _binParams[d].timeBin) {
              var binnedExpression = getBinnedDimExpression(
                dimArray[d],
                undefined,
                undefined,
                _binParams[d].timeBin
              );
              query += binnedExpression + " as key" + d.toString() + ",";
            } else {
              query += dimArray[d] + " as key" + d.toString() + ",";
            }
          }
          query += reduceExpression + checkForSortByAllRows() + " FROM " + _tablesStmt;

          function checkForSortByAllRows() {
            return sortByValue === "countval" ? ", COUNT(*) AS countval" : "";
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
          var filterQuery = writeFilter(queryBinParams);
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
          query += " GROUP BY ";
          for (var i = 0; i < dimArray.length; i++) {
            if (i !== 0) {
              query += ", ";
            }
            query += "key" + i.toString();
          }

          if (queryBinParams !== null) {
            if (_dataConnector.getPlatform() == "mapd") {
              var havingClause = " HAVING ";
              var hasBinParams = false;
              for (var d = 0; d < queryBinParams.length; d++) {
                if (queryBinParams[d] !== null && !queryBinParams[d].timeBin) {
                  if (d > 0 && hasBinParams) {
                    havingClause += " AND ";
                  }
                  hasBinParams = true;
                  havingClause += "key" + d.toString() + " >= 0 AND key" +
                  d.toString() + " < " + queryBinParams[d].numBins;
                }
              }
              if (hasBinParams) {
                query += havingClause;
              }
            } else {
              for (var d = 0; d < queryBinParams.length; d++) {
                if (queryBinParams[d] !== null) {
                  query += " HAVING " + binnedExpression + " >= 0 AND " +
                    binnedExpression + " < " + queryBinParams[d].numBins;
                }
              }
            }
          }

          // else {
          //   if (_dataConnector.getPlatform() == "mapd") {
          //     query += " HAVING key IS NOT NULL";
          //   } else {
          //     query += " HAVING " + dimensionExpression + " IS NOT NULL";
          //   }
          // }
          return query;
        }

        function setBoundByFilter(boundByFilterIn) {
          boundByFilter = boundByFilterIn;
          return group;
        }

        function numBins(numBinsIn) {
          if (!arguments.length) {
            var numBins = [];
            if (_binParams && _binParams.length) {
              for (var b = 0; b < _binParams.length; b++) {
                numBins.push(_binParams[b].numBins);
              }
            }
            return numBins;
          }
          if (!Array.isArray(numBinsIn))
              numBinsIn = [numBinsIn];
          if (numBinsIn.length != _binParams.length)
            throw ("Num bins length must be same as bin params length");
          for (var d = 0; d < numBinsIn.length; d++)
            _binParams[d].numBins = numBinsIn[d];
          return group;
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
            return _binParams;
          }
          _binParams = Array.isArray(binParamsIn) ? binParamsIn : [binParamsIn];
          return group;
        }

        function truncDate(dateLevel) {
          dateTruncLevel = dateLevel;

          // only for "variable" date trunc
          // TODO binCountIn always undefined
          binCount = binCountIn;

          return group;
        }

        function fillBins(queryBinParams, results) {
          if (!_fillMissingBins)
            return results;
          var numDimensions = queryBinParams.length;
          var numResults = results.length;
          var numTimeDims = 0;
          for (var d = 0; d < numDimensions; d++) {
            if (queryBinParams[d].timeBin) {
              numTimeDims++;
            }
          }
          var filledResults = [];

          // we only support filling bins when there is one time dimension
          // and it is the only dimension
          if (numDimensions == 1 && numTimeDims == 1) {
            //@todo fix this
            var actualTimeBinUnit = group.actualTimeBin(0);
            var incrementBy = 1;

            // convert non-supported time units to moment-compatible inputs
            // http://momentjs.com/docs/#/manipulating/
            switch (actualTimeBinUnit) {
              case "quarterday":
                actualTimeBinUnit = "hours";
                incrementBy = 6;
                break;
              case "decade":
                actualTimeBinUnit = "years";
                incrementBy = 10;
                break;
              case "century":
                actualTimeBinUnit = "years";
                incrementBy = 100;
                break;
              case "millenium":
                actualTimeBinUnit = "years";
                incrementBy = 1000;
                break;
            }
            var lastResult = null;
            var valueKeys = [];
            for (var r = 0; r < numResults; r++) {
              var result = results[r];
              if (lastResult) {
                var lastTime = lastResult.key0;
                var currentTime = moment(result.key0).utc().toDate();
                var nextTimeInterval = moment(lastTime)
                  .utc()
                  .add(incrementBy, actualTimeBinUnit)
                  .toDate();
                var interval = Math.abs(nextTimeInterval - lastTime);
                while (nextTimeInterval < currentTime) {
                  var timeDiff = currentTime - nextTimeInterval;
                  if (timeDiff > interval / 2) { // we have a missing time value
                    var insertResult = { key0: nextTimeInterval };
                    for (var k = 0; k < valueKeys.length; k++) {
                      insertResult[valueKeys[k]] = 0;
                    }
                    filledResults.push(insertResult);
                  }
                  nextTimeInterval = moment(nextTimeInterval)
                    .utc()
                    .add(incrementBy, actualTimeBinUnit)
                    .toDate();
                }
              } else { // first result - get its keys
                var allKeys = Object.keys(result);
                for (var k = 0; k < allKeys.length; k++) {
                  if (allKeys[k] !== "key0") {
                    valueKeys.push(allKeys[k]);
                  }
                }
              }
              filledResults.push(result);
              lastResult = result;
            }
            return filledResults;
          } else if (numTimeDims === 0 && numResults > 0) {
            var allDimsBinned = true; // we don't handle for now mixed cases
            var totalArraySize = 1;
            var dimensionSizes = [];
            var dimensionSums = [];
            for (var b = 0; b < queryBinParams.length; b++) {
              if (queryBinParams[b] === null) {
                allDimsBinned = false;
                break;
              }
              totalArraySize *= queryBinParams[b].numBins;
              dimensionSizes.push(queryBinParams[b].numBins);
              dimensionSums.push(b == 0 ? 1 : (queryBinParams[b].numBins * dimensionSums[b - 1]));
            }
            dimensionSums.reverse();
            if (allDimsBinned) {
              var numDimensions = dimensionSizes.length;

              // make an array filled with 0 of length numDimensions
              var counters = Array.apply(null, Array(numDimensions))
                .map(Number.prototype.valueOf, 0);
              var allKeys = Object.keys(results[0]);
              var valueKeys = [];
              for (var k = 0; k < allKeys.length; k++) {
                if (!allKeys[k].startsWith("key")) {
                  valueKeys.push(allKeys[k]);
                }
              }
              for (var i = 0; i < totalArraySize; i++) {
                var result = {};
                for (var k = 0; k < valueKeys.length; k++) {
                  result[valueKeys[k]] = 0; // Math.floor(Math.random() * 100);
                }
                for (var d = 0; d < numDimensions; d++) { // now add dimension keys
                  result["key" + d] = counters[d];
                }
                filledResults.push(result);
                for (var d = numDimensions - 1; d >= 0; d--) { // now add dimension keys
                  counters[d] += 1;
                  if (counters[d] < dimensionSizes[d]) {
                    break;
                  } else {
                    counters[d] = 0;
                  }
                }

              }
              for (var r = 0; r < numResults; r++) {
                var index = 0;
                for (var d = 0; d < numDimensions; d++) {
                  index += (results[r]["key" + d] * dimensionSums[d]);
                }
                filledResults[index] = results[r];
              }
              return (filledResults);
            }
          } else {
            return results;
          }
        }

        function unBinResults(queryBinParams, shouldFillBins, results) {
          if (shouldFillBins)
            results = fillBins(queryBinParams, results);
          var numRows = results.length;
          for (var b = 0; b < queryBinParams.length; b++) {
            if (queryBinParams[b] === null || queryBinParams[b].timeBin)
              continue;
            var queryBounds = queryBinParams[b].binBounds;
            var numBins = queryBinParams[b].numBins;

            // assuming rangeFilter is always more restrictive than boundByFilter
            if (boundByFilter && rangeFilters.length > 0) {
              queryBounds = rangeFilters[b];
            }
            var keyName = "key" + b.toString();

            var isDate = type(queryBounds[b]) == "date";
            if (isDate) {
              // in ms
              var unitsPerBin = (queryBounds[1].getTime() - queryBounds[0].getTime()) / numBins;
              var queryBounds0Epoch = queryBounds[0].getTime();
              for (var r = 0; r < numRows; ++r) {
                results[r][keyName] = new Date(
                  results[r][keyName] * unitsPerBin + queryBounds0Epoch
                );
              }
            } else {
              var unitsPerBin = (queryBounds[1] - queryBounds[0]) / numBins;
              for (var r = 0; r < numRows; ++r) {
                results[r][keyName] = (results[r][keyName] * unitsPerBin) + queryBounds[0];
              }
            }
          }
          return results;
        }

        /* set ordering to expression */
        function order(orderExpression) {
          _orderExpression = orderExpression;
          return group;
        }

        /* set ordering back to natural order (i.e. by measures)*/
        function orderNatural() {
          _orderExpression = null;
          return group;
        }

        function all(callbacks) {
          // freeze bin params so they don't change out from under us
          var queryBinParams = Array.isArray(_binParams) ? [].concat(_binParams) : [];
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
          var async = !!callbacks;
          var postProcessors = null;
          if (!!queryBinParams) {
            // true is for shouldFillBins
            postProcessors = [unBinResults.bind(this, queryBinParams, true)];
          }
          var options = {
            eliminateNullRows: eliminateNull,
            renderSpec: null,
            postProcessors: postProcessors,
            queryId: dimensionIndex,
          };

          if (async) {
            cache.queryAsync(query, options, callbacks);
          } else {
            return cache.query(query, options);
          }
        }

        function top(k, offset, renderSpec, callbacks) {
          // freeze bin params so they don't change out from under us
          var queryBinParams = Array.isArray(_binParams) ? [].concat(_binParams) : [];
          if (!queryBinParams.length) {
            queryBinParams = null;
          }
          var query = writeQuery(queryBinParams, _orderExpression);

          // could use alias "value" here
          query += " ORDER BY ";
          if (_orderExpression) {
            query += _orderExpression + " DESC";
          } else {
            var reduceArray = reduceVars.split(",");
            var reduceSize = reduceArray.length;
            for (var r = 0; r < reduceSize - 1; r++) {
              query += reduceArray[r] + " DESC,";
            }
            query += reduceArray[reduceSize - 1] + " DESC";
          }

          if (k != Infinity) {
            query += " LIMIT " + k;
          }
          if (offset !== undefined) {
            query += " OFFSET " + offset;
          }

          var async = !!callbacks;
          var postProcessors = null;
          if (!!queryBinParams) {
            // false is for shouldfillBins
            postProcessors = [unBinResults.bind(this, queryBinParams, false)];
          }
          var options = {
            eliminateNullRows: eliminateNull,
            renderSpec: null,
            postProcessors: postProcessors,
            queryId: dimensionIndex,
          };

          if (async) {
            cache.queryAsync(query, options, callbacks);
          } else {
            return cache.query(query, options);
          }
        }

        function bottom(k, offset, renderSpec, callbacks) {

          // freeze bin params so they don't change out from under us
          var queryBinParams = Array.isArray(_binParams) ? [].concat(_binParams) : [];
          if (!queryBinParams.length) {
            queryBinParams = null;
          }
          var query = writeQuery(queryBinParams, _orderExpression);
          query += " ORDER BY ";
          if (_orderExpression) {
            query += _orderExpression;
          } else {
            var reduceArray = reduceVars.split(",");
            var reduceSize = reduceArray.length;
            for (var r = 0; r < reduceSize - 1; r++) {
              query += reduceArray[r] + ",";
            }
            query += reduceArray[reduceSize - 1];
          }

          if (k != Infinity) {
            query += " LIMIT " + k;
          }
          if (offset !== undefined)
            query += " OFFSET " + offset;

          var async = !!callbacks;
          var postProcessors = null;
          if (!!queryBinParams) {
            // false is for shouldFillBins
            postProcessors = [unBinResults.bind(this, queryBinParams, false)];
          }
          var options = {
            eliminateNullRows: eliminateNull,
            renderSpec: null,
            postProcessors: postProcessors,
            queryId: dimensionIndex,
          };
          if (async)
            cache.queryAsync(query, options, callbacks);
          else
            return cache.query(query, options);
        }

        function reduceCount(countExpression) {
          reduce([{ expression: countExpression, agg_mode: "count", name: "val" }]);
          return group;
        }

        function reduceSum(sumExpression) {
          reduce([{ expression: sumExpression, agg_mode: "sum", name: "val" }]);
          return group;
        }

        function reduceAvg(avgExpression) {
          reduce([{ expression: avgExpression, agg_mode: "avg", name: "val" }]);
          return group;
        }

        function reduceMin(minExpression) {
          reduce([{ expression: minExpression, agg_mode: "min", name: "val" }]);
          return group;
        }

        function reduceMax(maxExpression) {
          reduce([{ expression: maxExpression, agg_mode: "max", name: "val" }]);
          return group;
        }

        // expressions should be an array of
        // { expression, agg_mode (sql_aggregate), name, filter (optional) }
        function reduce(expressions) {
          // _reduceTableSet = {};

          if (!arguments.length) {
            return reduceSubExpressions;
          }
          reduceSubExpressions = expressions;
          reduceExpression = "";
          reduceVars = "";
          var numExpressions = expressions.length;
          for (var e = 0; e < numExpressions; e++) {
            if (e > 0) {
              reduceExpression += ",";
              reduceVars += ",";
            }
            if (e == targetSlot
                && targetFilter != null
                && targetFilter != dimensionIndex
                && filters[targetFilter] != "") {

              // this is the old way
              // reduceExpression += "AVG(CAST(" + filters[targetFilter] + " AS INT))"
              reduceExpression += " AVG(CASE WHEN " + filters[targetFilter] + " THEN 1 ELSE 0 END)";
            } else {

              /*
               * if (expressions[e].expression in columnTypeMap) {
               *   _reduceTableSet[columnTypeMap[expressions[e].expression].table] =
               *     (_reduceTableSet[columnTypeMap[expressions[e].expression].table] || 0) + 1;
               *  }
               */

              var agg_mode = expressions[e].agg_mode.toUpperCase();

              if (agg_mode === "CUSTOM") {
                reduceExpression += expressions[e].expression;
              } else if (agg_mode == "COUNT") {
                if (expressions[e].filter) {
                  reduceExpression += "COUNT(CASE WHEN " + expressions[e].filter + " THEN 1 END)";
                } else {
                  if (typeof expressions[e].expression !== "undefined") {
                    reduceExpression += "COUNT(" + expressions[e].expression + ")";
                  } else {
                    reduceExpression += "COUNT(*)";
                  }
                }
              } else { // should check for either sum, avg, min, max
                if (expressions[e].filter) {
                  reduceExpression += agg_mode + "(CASE WHEN " + expressions[e].filter +
                    " THEN " +  expressions[e].expression + " END)";
                } else {
                  reduceExpression += agg_mode + "(" + expressions[e].expression + ")";
                }
              }
            }
            reduceExpression += " AS " + expressions[e].name;
            reduceVars += expressions[e].name;
          }
          return group;
        }

        function size(ignoreFilters) {
          var query = "SELECT ";
          for (var d = 0; d < dimArray.length; d++) {
            if (d > 0) {
              query += ",";
            }
            query += "COUNT(DISTINCT " + dimArray[d] + ") AS n";
            if (multiDim) {
              query += d.toString();
            }
          }
          query += " FROM " + _tablesStmt;
          if (!ignoreFilters) {

            // freeze bin params so they don"t change out from under us
            var queryBinParams = Array.isArray(_binParams) ? [].concat(_binParams) : [];
            if (!queryBinParams.length) {
              queryBinParams = null;
            }
            var filterQuery = writeFilter(queryBinParams);
            if (filterQuery != "") {
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
          } else {
            if (_joinStmt !== null) {
              query += " WHERE " + _joinStmt;
            }
          }
          if (!multiDim) {
            return _dataConnector.query(query)[0].n;
          } else {
            var queryResult = _dataConnector.query(query)[0];
            var result = [];
            for (var d = 0; d < dimArray.length; d++) {
              var varName = "n" + d.toString();
              result.push(queryResult[varName]);
            }
            return result;
          }
        }
        return reduceCount();
      }

      function dispose() {
        filters[dimensionIndex] = null;
        dimensions[dimensionIndex] = null;
      }

      var nonAliasedDimExpression = "";

      dimensions.push(dimensionExpression);
      for (var d = 0; d < dimArray.length; d++) {
        if (dimArray[d] in columnTypeMap) {
          dimContainsArray[d] = columnTypeMap[dimArray[d]].is_array;
        } else if (dimArray[d] in compoundColumnMap) {
          dimContainsArray[d] = columnTypeMap[compoundColumnMap[dimArray[d]]].is_array;
        } else {
          dimContainsArray[d] = false;
        }
      }
      return dimension;
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
        getReduceExpression: function () {return reduceExpression; }, // TODO for testing only
      };
      var reduceExpression = null;
      var maxCacheSize = 5;
      var cache = resultCache(_dataConnector);

      function writeFilter() {
        var filterQuery = "";
        var validFilterCount = 0;

        // we observe all filters
        for (var i = 0; i < filters.length; i++) {
          if (filters[i] && filters[i] != "") {
            if (validFilterCount > 0) {
              filterQuery += " AND ";
            }
            validFilterCount++;
            filterQuery += filters[i];
          }
        }
        return filterQuery;
      }

      function writeQuery(ignoreFilters) {
        var query = "SELECT " + reduceExpression + " FROM " + _tablesStmt;
        if (!ignoreFilters) {
          var filterQuery = writeFilter();
          if (filterQuery != "") {
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
        } else {
          if (_joinStmt !== null) {
            query += " WHERE " + _joinStmt;
          }
        }

        // could use alias "key" here
        return query;
      }

      function reduceCount(countExpression) {
        if (typeof countExpression !== "undefined")
          reduceExpression = "COUNT(" + countExpression + ") as val";
        else
          reduceExpression = "COUNT(*) as val";
        return group;
      }

      function reduceSum(sumExpression) {
        reduceExpression = "SUM(" + sumExpression + ") as val";
        return group;
      }

      function reduceAvg(avgExpression) {
        reduceExpression = "AVG(" + avgExpression + ") as val";
        return group;
      }

      function reduceMin(minExpression) {
        reduceExpression = "MIN(" + minExpression + ") as val";
        return group;
      }

      function reduceMax(maxExpression) {
        reduceExpression = "MAX(" + maxExpression + ") as val";
        return group;
      }

      function reduce(expressions) {
        //expressions should be an array of {expression, agg_mode (sql_aggregate), name}
        reduceExpression = "";
        var numExpressions = expressions.length;
        for (var e = 0; e < numExpressions; e++) {
          if (e > 0) {
            reduceExpression += ",";
          }
          var agg_mode = expressions[e].agg_mode.toUpperCase();
          if (agg_mode == "COUNT") {
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

      function value(ignoreFilters) {
        var query = writeQuery(ignoreFilters);
        var options = {
          eliminateNullRows: false,
          renderSpec: null,
          postProcessors: [function (d) {return d[0].val;}],
          queryId: -1,
        };
        return cache.query(query, options);
      }

      function valueAsync(callbacks) {
        var query = writeQuery();
        var options = {
          eliminateNullRows: false,
          renderSpec: null,
          postProcessors: [function (d) {return d[0].val;}],
          queryId: -1,
        };
        cache.queryAsync(query, options, callbacks);
      }

      function values(ignoreFilters) {
        var query = writeQuery(ignoreFilters);
        var options = {
          eliminateNullRows: false,
          renderSpec: null,
          postProcessors: [function (d) {return d[0];}],
          queryId: -1,
        };
        return cache.query(query, options);
      }

      return reduceCount();
    }

    // Returns the number of records in this crossfilter, irrespective of any filters.
    function size() {
      var query = "SELECT COUNT(*) as n FROM " + _tablesStmt;
      if (_joinStmt !== null) {
        query += " WHERE " + _joinStmt;
      }

      var options = {
        eliminateNullRows: false,
        renderSpec: null,
        postProcessors: [function (d) {return d[0].n;}],
      };
      return cache.query(query, options);
    }

    return (arguments.length >= 2)
      ? setData(arguments[0], arguments[1], arguments[2]) // dataConnector, dataTable
      : crossfilter;
  }
})(typeof exports !== "undefined" && exports || this);
