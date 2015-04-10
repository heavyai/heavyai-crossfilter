function createDateAsUTC(date) {
      return new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate(), date.getHours(), date.getMinutes(), date.getSeconds()));
          }

(function(exports){
crossfilter.version = "1.3.11";


exports.crossfilter=crossfilter;
function crossfilter() {

  var crossfilter = {
    type: 'crossfilter',
    setData: setData, 
    filter: filter,
    getColumns:getColumns,
    dimension: dimension,
    groupAll: groupAll,
    size: size,
    getFilter: function() {return filters;},
    getDimensions: function() {return dimensions;},
    getTableLabel: function() {return tableLabel;}
  };

  var dataTable = null;
  var filters = [];
  var columnTypeMap = null;
  var tableLabel = null;
  var dataConnector = null;
  var dimensions = [];
  var globalFilters = [];

  var TYPES = {
      'undefined'        : 'undefined',
      'number'           : 'number',
      'boolean'          : 'boolean',
      'string'           : 'string',
      '[object Function]': 'function',
      '[object RegExp]'  : 'regexp',
      '[object Array]'   : 'array',
      '[object Date]'    : 'date',
      '[object Error]'   : 'error'
  },
  TOSTRING = Object.prototype.toString;

  function type(o) {
        return TYPES[typeof o] || TYPES[TOSTRING.call(o)] || (o ? 'object' : 'null');
  };


  
  function setData(newDataConnector, newDataTable, newTableLabel) {
    dataConnector = newDataConnector;
    dataTable = newDataTable;
    tableLabel = newTableLabel;
    var columnsArray = dataConnector.getFields(dataTable);
    columnTypeMap = {};

    columnsArray.forEach(function (element) {
      columnTypeMap[element.name] = element.type;
    });
    return crossfilter;
  }

  function getColumns() {
    return columnTypeMap;
  }

  function filter() {
    var filter = {
      filter: filter,
      filterAll: filterAll,
      getFilter: getFilter
    }

    var filterIndex = filters.length;
    filters.push("");

    function getFilter() {
      return filters[filterIndex];
    }

    function filter(filterExpr) {
      if (filterExpr == undefined || filterExpr ==  null) {
        filterAll();
      }
      else {
        filters[filterIndex] = filterExpr;
      }
    }

    function filterAll() {
      filters[filterIndex] = "";
    }

    return filter;
  }


  function dimension(expression) {
    var dimension = {
      type: 'dimension',
      filter: filter,
      filterExact: filterExact,
      filterRange: filterRange,
      filterAll: filterAll,
      filterDisjunct: filterDisjunct,
      filterLike: filterLike,
      filterILike: filterILike,
      getFilter: getFilter,
      projectOn: projectOn,
      projectOnAllDimensions: projectOnAllDimensions,
      getResultSet: function() {return resultSet;},
      top: top,
      bottom: bottom,
      group: group,
      groupAll: groupAll,
      dispose: dispose,
      remove: dispose,
    };
    var dimensionIndex = filters.length;  
    var dimensionGroups = [];
    filters.push("");
    var dimensionExpression = expression;
    var projectExpressions = [];
    var projectOnAllDimensions = false;
    var binBounds = null; // for binning
    var rangeFilter = null;
    var resultSet = null;
    //var resetRange = false;
    /*
    var filterExpression = null;
    var exactFilter = null;
    var rangeFilter = null;
    var functionFilter = null;
    var filterType = null;
    */

    /*
    function filterIndexBounds(bounds) {
      lo0 = bounds[0];
      lo1 = bounds[1];
      return dimension;
    }
    */
    function projectOn(expressions) {
      projectExpressions = expressions;
      return dimension;
    }

    function projectOnAllDimensions(flag) {
      projectOnAllDimensionsFlag = flag;
      return dimension;
    }

    function getFilter() {
      return filters[dimensionIndex];
    }

    function filter(range, append,resetRange) {
      append = typeof append !== 'undefined' ? append : false;
      return range == null
          ? filterAll() : Array.isArray(range)
          ? filterRange(range, append,resetRange) : typeof range === "function"
          ? filterFunction(range, append)
          : filterExact(range, append);

    }


    function formatFilterValue(value) {
      var valueType = type(value);
      if (valueType == "string") {
        return "'" + value + "'";
      }
      else if (valueType == "date") {
        return "TIMESTAMP(0) '" + value.toISOString().slice(0,19).replace('T',' ') + "'"
      }
      else {
        return value;
      }
    }

    function filterExact(value,append) {
      append = typeof append !== 'undefined' ? append : false;
      var typedValue = formatFilterValue(value);
      if (append) {
        filters[dimensionIndex] += dimensionExpression + " = " + typedValue; 
      }
      else {
        filters[dimensionIndex] = dimensionExpression + " = " + typedValue; 
      }
      return dimension;
    }

    function filterLike(value,append) {
      append = typeof append !== 'undefined' ? append : false;
      if (append) {
          filters[dimensionIndex] += dimensionExpression + " like '%" + value + "%'"; 
      }
      else {
          filters[dimensionIndex] = dimensionExpression + " like '%" + value + "%'"; 
      }
    }

    function filterILike(value) {
      append = typeof append !== 'undefined' ? append : false;
      if (append) {
          filters[dimensionIndex] += dimensionExpression + " ilike '%" + value + "%'"; 
      }
      else {
          filters[dimensionIndex] = dimensionExpression + " ilike '%" + value + "%'"; 
      }
    }

    function filterRange(range, append,resetRange) {
      append = typeof append !== 'undefined' ? append : false;
      if (resetRange == true) {
        rangeFilter = range;
      }

      //rangeFilter = range;
      var typedRange = [formatFilterValue(range[0]),formatFilterValue(range[1])];
      if (append) {
        filters[dimensionIndex] += "(" + dimensionExpression + " >= " + typedRange[0] + " AND " + dimensionExpression + " < " + typedRange[1] + ")"; 
      }
      else {
        filters[dimensionIndex] = "(" + dimensionExpression + " >= " + typedRange[0] + " AND " + dimensionExpression + " < " + typedRange[1] + ")"; 
      }
      return dimension;

    }

    function filterDisjunct(disjunctFilters,resetRangeIn) { // applying or with multiple filters"
      var filterWasNull = filters[dimensionIndex] == null || filters[dimensionIndex] == "";
      var resetRange = false;
      if (resetRangeIn != undefined) {
        resetRange = resetRangeIn; 
        if (resetRange == true) {
          $(dimension).trigger("reranged");
        }
      }

      var lastFilterIndex = disjunctFilters.length - 1;
      filters[dimensionIndex] = "(";
      
      for (var i = 0; i <= lastFilterIndex; i++) {
        var curFilter = disjunctFilters[i]; 
        filter(curFilter, true,resetRange);
        /*
        if (Array.isArray(filter)) {
          filters[dimensionIndex] += dimensionExpression + " >= " + filter[0] + " AND " + dimensionExpression + " < " + filter[1]; 
        }
        else {
          filters[dimensionIndex] += dimensionExpression + " = " + filter;
        }
        */
        if (i != lastFilterIndex) {
          filters[dimensionIndex] += " OR ";
        }
      }
      filters[dimensionIndex] += ")";
      var filterNowNull = filters[dimensionIndex] == null || filters[dimensionIndex] == "";
      if (filterWasNull && !filterNowNull) {
        $(this).trigger("filter-on");
      }
      else if (!filterWasNull && filterNowNull) {
        $(this).trigger("filter-clear");
      }
      return dimension;
    }
    /*
    function filterFunction(f) {
      filterFunction = f;
      filterType = "function";
      return dimension;
    }
    */

    function filterAll(softFilterClear) {
      if (softFilterClear == undefined || softFilterClear == false) {
        $(this).trigger("filter-clear");
        rangeFilter = null;
      }
      filters[dimensionIndex] = "";
      return dimension;
    }

    // Returns the top K selected records based on this dimension's order.
    // Note: observes this dimension's filter, unlike group and groupAll.
    function writeQuery() {
      if (projectExpressions.length == 0) {
        return null;
      }
      var projList = "";
      if (projectOnAllDimensionsFlag) {
        var dimensions = crossfilter.getDimensions();
        var nonNullDimensions = [];
        for (var d = 0; d < dimensions.length; d++) {
          if (dimensions[d] != null) {
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
        projList = nonNullDimensions.join(",")
        console.log(projList);
      }
      else {
        projList = projectExpressions.join(",");
      }

      var query = "SELECT " + projList + " FROM " + dataTable;
      var filterQuery = "";
      var nonNullFilterCount = 0;
      // we observe this dimensions filter
      for (var i = 0; i < filters.length ; i++) {
        if (filters[i] && filters[i] != "") {
          if (nonNullFilterCount > 0) {
            filterQuery += " AND ";
          }
          nonNullFilterCount++;
          filterQuery += filters[i];
        }
      }
      if (filterQuery != "") {
        query += " WHERE " + filterQuery;
      }
      return query;
    }


    function top(k,callback) {
      var query = writeQuery();
      if (query == null) {
        return {};
      }

      if (dimensionExpression != null) {
        query += " ORDER BY " + dimensionExpression + " LIMIT " + k; 
        return dataConnector.query(query);
      }
      else {
        query += " LIMIT " + k; 
        resultSet =  dataConnector.query(query);
        return resultSet;
      }

      /*
      if (callback == null) {
        resultSet = dataConnector.query(query);
      }
      else {
        resultSet = dataConnector.queryAsync(query,callback)
      }
      return resultSet;
      */
    }

    function bottom(k) {
      var query = writeQuery();
      console.log(query);
      if (query == null) {
        return {};
      }

      if (dimensionExpression != null) {
        query += " ORDER BY " + dimensionExpression + " DESC LIMIT " + k; 
        return dataConnector.query(query);
      }
      else { 
        query += " LIMIT " + k; 
        resultSet = dataConnector.query(query);
        return resultSet;
      }
    }

    function group() {
      var group = {
        top: top,
        all: all,
        numBins: numBins,
        truncDate: truncDate,
        //reduce: reduce,
        reduceCount: reduceCount,
        reduceSum: reduceSum,
        reduceAvg: reduceAvg,
        reduceMin: reduceMin,
        reduceMax: reduceMax,
        reduceMulti: reduceMulti,
        setBoundByFilter: setBoundByFilter,
        having: having,
        //order: order,
        //orderNatural: orderNatural,
        size: size,
        lastTopQuery: function() {return lastTopQuery},
        lastAllQuery: function() {return lastAllQuery},
        //dispose: dispose,
        //remove: dispose // for backwards-compatibility
      };
      var reduceExpression = null;  // count will become default
      var reduceVars = null;
      var havingExpression = null;
      var binCount = null;
      var boundByFilter = false;
      var dateTruncLevel = null;
      var lastTopQuery = null;
      var lastAllQuery = null;
      var lastTopResults = null;
      var lastAllResults = null;


      dimensionGroups.push(group);

      function writeFilter() {
        var filterQuery = "";
        var nonNullFilterCount = 0;
        // we do not observe this dimensions filter
        for (var i = 0; i < filters.length ; i++) {
          if (i != dimensionIndex  && filters[i] && filters[i] != "") {
            if (nonNullFilterCount > 0) {
              filterQuery += " AND ";
            }
            nonNullFilterCount++;
            filterQuery += filters[i];
          }
          else if (i == dimensionIndex && binCount != null) {
            if (nonNullFilterCount > 0) {
              filterQuery += " AND ";
            }
            nonNullFilterCount++;
            var queryBounds = binBounds;
            if (boundByFilter == true && rangeFilter != null) {
              queryBounds = rangeFilter;
            }

            
            filterQuery += "(" + dimensionExpression +  " >= " + formatFilterValue(queryBounds[0]) + " AND " + dimensionExpression + " < " + formatFilterValue(queryBounds[1]) + ")";
          }
        }
        return filterQuery;
      }

      function getBinnedDimExpression() {
        var queryBounds = binBounds;
        if (boundByFilter && rangeFilter != null) {
          queryBounds = rangeFilter;
        }
        var isDate = type(queryBounds[0]) == "date";
        if (isDate) {
          var dimExpr = "extract(epoch from " + dimensionExpression + ")";
          var filterRange = (queryBounds[1].getTime() - queryBounds[0].getTime()) * 0.001; // as javscript epoch is in ms
        var binsPerUnit = binCount/filterRange; // is this a float in js?
        //var lowerBoundsUTC = createDateAsUTC(queryBounds[0]).getTime()/1000;
        var lowerBoundsUTC = queryBounds[0].getTime()/1000;
        var binnedExpression = "cast((" + dimExpr + " - " + lowerBoundsUTC + ") *" + binsPerUnit + " as int)";
        return binnedExpression;
        }
        else {
          var filterRange = queryBounds[1] - queryBounds[0];
          var binsPerUnit = binCount/filterRange; // is this a float in js?
          var binnedExpression = "cast((" + dimensionExpression + " - " + queryBounds[0] + ") *" + binsPerUnit + " as int)";
          return binnedExpression;
        }
      }

      function getDateTruncLevel (timeRange,maxNumBins) {
        //timeRange is in seconds
        if (timeRange < maxNumBins)
          return 'second';
        if (timeRange / 60 < maxNumBins)
          return 'minute';
        if (timeRange / 3600 < maxNumBins)
          return 'hour';
        if (timeRange / 86400  < maxNumBins)
          return 'day';
        if (timeRange / 2592000  < maxNumBins)
          return 'month';
        return 'year';
      }

      function getDateTruncExpression() {
        var dateTrunc = dateTruncLevel;
        if (dateTruncLevel == "variable") {
          // we expect binBounds and binCount to be populated
          var dateTrunc = getDateTruncLevel(timeRange,binCount);
        }
        return "date_trunc('" + dateTrunc + "'," + dimensionExpression + ")";
      }

      function writeQuery() {
        var query = null;
        /*
        if (dateTruncLevel != null) {
          query = "SELECT " + getDateTruncExpression() + " as key," + reduceExpression + " FROM " + dataTable ;

        }
        */
        var binnedExpression = null;
        if (binCount != null) {
          binnedExpression = getBinnedDimExpression();
          query = "SELECT " + binnedExpression + " as key," + reduceExpression + " FROM " + dataTable ;
        }
        else {
          query = "SELECT " + dimensionExpression + " as key," + reduceExpression + " FROM " + dataTable ;
        }
        var filterQuery = writeFilter(); 
        if (filterQuery != "") {
          query += " WHERE " + filterQuery;
        }
        // could use alias "key" here
        query += " GROUP BY key";
        if (binCount != null) {
          if (dataConnector.getPlatform() == "mapd") {
            query += " HAVING key >= 0 AND key < " + binCount;
          }
          else {
            query += " HAVING " + binnedExpression + " >= 0 AND " + binnedExpression + " < " + binCount;
          }
        }
        else {
          if (dataConnector.getPlatform() == "mapd") {
            query += " HAVING key IS NOT NULL";
          }
          else {
            query += " HAVING " + dimensionExpression + " IS NOT NULL";
          }
        }

        /*
        if (havingExpression != null) {
          query += " HAVING " + havingExpression;
        }
        */
        return query;
      }

      function setBoundByFilter(boundByFilterIn) {
        boundByFilter = boundByFilterIn;
        return group;
      }

      function setAnimFilter() {

        return group;
      }

      function numBins(binCountIn,initialBounds, boundByFilterIn) {
        binCount = binCountIn;
        binBounds = initialBounds;
        if (boundByFilterIn != undefined) {
          boundByFilter = boundByFilterIn;
        }
        return group;
      }

      function truncDate(dateLevel) {
        dateTruncLevel = dateLevel;
        binCount = binCountIn; // only for "variable" date trunc
        return group;
      }

      function unBinResults(results) {
        var numRows = results.length;
        var queryBounds = binBounds;
        if (boundByFilter && rangeFilter != null) {
          queryBounds = rangeFilter;
        }
        var isDate = type(queryBounds[0]) == "date";


        if (isDate) {
          var unitsPerBin = (queryBounds[1].getTime()-queryBounds[0].getTime())/binCount; // in ms
        var queryBounds0Epoch = queryBounds[0].getTime();
          for (var r = 0; r < numRows; ++r) { 
            results[r]["key"] = new Date ( results[r]["key"] * unitsPerBin + queryBounds0Epoch);
          }


        }
        else {
          var unitsPerBin = (queryBounds[1]-queryBounds[0])/binCount;
          for (var r = 0; r < numRows; ++r) { 
            results[r]["key"] = (results[r]["key"] * unitsPerBin) + queryBounds[0];
          }
        }
        return results;
      }


      function all() {
        var query = writeQuery();
        // could use alias "key" here
        //query += " ORDER BY " + dimensionExpression;
        query += " ORDER BY key";
        if (lastAllQuery == query) {
          return lastAllResults;
        }
        lastAllQuery = query;
        if (binCount != null) {
          lastAllResults = unBinResults(dataConnector.query(query));
          return lastAllResults;
        }
        else {
          lastAllResults = dataConnector.query(query);
          return lastAllResults;
          //return dataConnector.query(query);
        }
      }


      function top(k) {
        var query = writeQuery();
        // could use alias "value" here
        query += " ORDER BY ";
        var reduceArray = reduceVars.split(',')
        var reduceSize = reduceArray.length;
        for (var r = 0; r < reduceSize - 1; r++) {
          query += reduceArray[r] +" DESC,";
        }
          query += reduceArray[reduceSize-1] +" DESC";
        if (k != Infinity) {
          query += " LIMIT " + k;
        }
        if (lastTopQuery == query) {
          return lastTopResults;
          //return null;
        }
        lastTopQuery = query;
        lastTopResults = dataConnector.query(query);
        return lastTopResults;
      }

      function bottom(k) {
        var query = writeQuery();
        // could use alias "value" here
        query += " ORDER BY " + reduceVars;
        return dataConnector.query(query);
      }

      function reduceCount() {
        reduceExpression = "COUNT(*) AS value";  
        reduceVars = "value";
        return group;
      }

      function reduceSum(sumExpression) {
        reduceExpression = "SUM(" + sumExpression + ") AS value";
        reduceVars = "value";
        return group;
      }

      function reduceAvg(avgExpression) {
        reduceExpression = "AVG(" + avgExpression +") AS value";  
        reduceVars = "value";
        return group;
      }

      function reduceMin(minExpression) {
        reduceExpression = "MIN(" + minExpression +") AS value";  
        reduceVars = "value";
        return group;
      }

      function reduceMax(maxExpression) {
        reduceExpression = "MAX(" + maxExpression +") AS value";  
        reduceVars = "value";
        return group;
      }

      function reduceMulti(expressions) {
        //expressions should be an array of {expression, agg_mode (sql_aggregate), name} 
        reduceExpression = "";
        reduceVars = "";
        var numExpressions = expressions.length;
        for (var e = 0; e < numExpressions; e++) {
          if (e > 0) {
            reduceExpression += ",";
            reduceVars += ",";
          }
          var agg_mode = expressions[e].agg_mode.toUpperCase();
          if (agg_mode == "COUNT") {
            reduceExpression += "COUNT(*)";
          }
          else { // should check for either sum, avg, min, max
            reduceExpression += agg_mode + "(" + expressions[e].expression + ")";
          }
          reduceExpression += " AS " + expressions[e].name;
          reduceVars += expressions[e].name;
          //reduceExpressionMap[expressions[e].name] = expressions[e
        }
        return group;
      }

      function having(expression) {
        havingExpression=expression;
        return group;
      }
        

      function size(ignoreFilters) {
        var query = "SELECT COUNT(DISTINCT " + dimensionExpression + ") AS n FROM " + dataTable;
        if (!ignoreFilters) {
          var filterQuery = writeFilter(); 
          if (filterQuery != "") {
            query += " WHERE " + filterQuery;
          }
        }
        return dataConnector.query(query)[0]['n'];
        //return 7;
      }

      return reduceCount();
    }

    function dispose() {
      filters[dimensionIndex] = null;
      dimensions[dimensionIndex] = null;
      //filters.splice(dimensionIndex,1);
    }
    
    dimensions.push(dimensionExpression);
    return dimension;
  }
  function groupAll() {
    var group = {
      reduceCount: reduceCount,
      reduceSum: reduceSum,
      reduceAvg: reduceAvg,
      reduceMin: reduceMin,
      reduceMax: reduceMax,
      reduceMulti: reduceMulti,
      value: value,
      values: values,
      //dispose: dispose,
      //remove: dispose // for backwards-compatibility
    };
    var reduceExpression = null; 
    
    function writeFilter() {
      var filterQuery = "";
      var validFilterCount = 0;
      // we observe all filters
      for (var i = 0; i < filters.length ; i++) {
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
      var query = "SELECT " + reduceExpression + " FROM " + dataTable ;
      if (!ignoreFilters) {
        var filterQuery = writeFilter(); 
        if (filterQuery != "") {
          query += " WHERE " + filterQuery;
        }
      }
      // could use alias "key" here
      //query += " GROUP BY " +  dimensionExpression;
      return query;
    }

    function reduceCount() {
      reduceExpression = "COUNT(*) as value";  
      return group;
    }

    function reduceSum(sumExpression) {
      reduceExpression = "SUM(" + sumExpression + ") as value";
      return group;
    }

    function reduceAvg(avgExpression) {
      reduceExpression = "AVG(" + avgExpression +") as value";  
      return group;
    }

    function reduceMin(minExpression) {
      reduceExpression = "MIN(" + minExpression +") as value";  
      return group;
    }

    function reduceMax(maxExpression) {
      reduceExpression = "MAX(" + maxExpression +") as value";  
      return group;
    }

    function reduceMulti(expressions) {
      //expressions should be an array of {expression, agg_mode (sql_aggregate), name} 
        reduceExpression = "";
        var numExpressions = expressions.length;
        for (var e = 0; e < numExpressions; e++) {
          if (e > 0) {
            reduceExpression += ",";
          }
          var agg_mode = expressions[e].agg_mode.toUpperCase();
          if (agg_mode == "COUNT") {
            reduceExpression += "COUNT(*)";
          }
          else { // should check for either sum, avg, min, max
            reduceExpression += agg_mode + "(" + expressions[e].expression + ")";
          }
          reduceExpression += " AS " + expressions[e].name;
          //reduceExpressionMap[expressions[e].name] = expressions[e
        }
        return group;
      }
      //
      //
      //

    function value(ignoreFilters) {
      var query = writeQuery(ignoreFilters);
      // Below works because result set will be one field with one row
      return dataConnector.query(query)[0]['value'];
    }

    function values(ignoreFilters) {
      var query = writeQuery(ignoreFilters);
      return dataConnector.query(query)[0];
    }

    return reduceCount();
  }


  // Returns the number of records in this crossfilter, irrespective of any filters.
  function size() {
    var query = "SELECT COUNT(*) as n FROM " + dataTable;
    return dataConnector.query(query)[0]['n'];
  }

  return (arguments.length == 3)
    ? setData(arguments[0],arguments[1], arguments[2]) // dataConnector, dataTable
    : crossfilter;

}
})(typeof exports !== 'undefined' && exports || this);
