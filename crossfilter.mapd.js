(function(exports){
crossfilter.version = "1.3.11";


exports.crossfilter=crossfilter;
function crossfilter() {

  var crossfilter = {
    setData: setData, 
    dimension: dimension,
    groupAll: groupAll,
    size: size
  };

  var dataTable = null;
  var filters = [];
  var colummnTypeMap = null;

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


  
  function setData(newDataConnector, newDataTable) {
    dataConnector = newDataConnector;
    dataTable = newDataTable;
    var columnsArray = dataConnector.getFields(dataTable);
    columnTypeMap = {};
    columnsArray.forEach(function (element) {
      columnTypeMap[element.name] = element.type;
    });
    return crossfilter;
  }

  function dimension(expression) {
    var dimension = {
      filter: filter,
      filterExact: filterExact,
      filterRange: filterRange,
      filterAll: filterAll,
      filterDisjunct: filterDisjunct,
      top: top,
      bottom: bottom,
      group: group,
      groupAll: groupAll,
      dispose: dispose
    };
    var dimensionIndex = filters.length;  
    var dimensionGroups = [];
    filters.push("");
    var dimensionExpression = expression;
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

    function filter(range, append) {
      append = typeof append !== 'undefined' ? append : false;
      return range == null
          ? filterAll() : Array.isArray(range)
          ? filterRange(range, append) : typeof range === "function"
          ? filterFunction(range, append)
          : filterExact(range,append);
    }


    function formatFilterValue(value) {
      var valueType = type(value);
      if (valueType == "string") {
        return "'" + value + "'";
      }
      else if (valueType == "date") {
        return "'" + value.toISOString().slice(0,19).replace('T',' ') + "'"
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

    function filterRange(range, append) {
      append = typeof append !== 'undefined' ? append : false;
      var typedRange = [formatFilterValue(range[0]),formatFilterValue(range[1])];
      if (append) {
        filters[dimensionIndex] += dimensionExpression + " >= " + typedRange[0] + " AND " + dimensionExpression + " < " + typedRange[1]; 
      }
      else {
        filters[dimensionIndex] = dimensionExpression + " >= " + range[0] + " AND " + dimensionExpression + " < " + range[1]; 
      }
      return dimension;

    }

    function filterDisjunct(disjunctFilters) { // applying or with multiple filters"
      var lastFilterIndex = disjunctFilters.length - 1;
      for (var i = 0; i <= lastFilterIndex; i++) {
        var curFilter = disjunctFilters[i]; 
        filter(curFilter, true);
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
      return dimension;
    }
    /*
    function filterFunction(f) {
      filterFunction = f;
      filterType = "function";
      return dimension;
    }
    */

    function filterAll() {
      filters[dimensionIndex] = "";
      return dimension;
    }

    // Returns the top K selected records based on this dimension's order.
    // Note: observes this dimension's filter, unlike group and groupAll.
    function writeQuery() {
      var query = "SELECT * FROM " + dataTable;
      var filterQuery = "";
      var nonNullFilterCount = 0;
      // we observe this dimensions filter
      for (var i = 0; i < filters.length ; i++) {
        if (filters[i] != "") {
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


    function top(k) {
      var query = writeQuery();
      query += " ORDER BY " + dimensionExpression + " LIMIT " + k; 
      return dataConnector.query(query);
    }

    function bottom(k) {
      var query = writeQuery();
      query += " ORDER BY " + dimensionExpression + " DESC LIMIT " + k; 
      return dataConnector.query(query);
    }

    function group(key) {
      var group = {
        top: top,
        all: all,
        //reduce: reduce,
        reduceCount: reduceCount,
        reduceSum: reduceSum,
        reduceAvg: reduceAvg,
        //order: order,
        //orderNatural: orderNatural,
        size: size,
        //dispose: dispose,
        //remove: dispose // for backwards-compatibility
      };
      var reduceExpression = null;  // count will become default

      dimensionGroups.push(group);

      function writeFilter() {
        var filterQuery = "";
        var nonNullFilterCount = 0;
        // we do not observe this dimensions filter
        for (var i = 0; i < filters.length ; i++) {
          if (i != dimensionIndex && filters[i] != "") {
            if (nonNullFilterCount > 0) {
              filterQuery += " AND ";
            }
            nonNullFilterCount++;
            filterQuery += filters[i];
          }
        }
        return filterQuery;
      }

      function writeQuery() {
        var query = "SELECT " + dimensionExpression + " as key," + reduceExpression + " AS value FROM " + dataTable ;
        var filterQuery = writeFilter(); 
        if (filterQuery != "") {
          query += " WHERE " + filterQuery;
        }
        // could use alias "key" here
        query += " GROUP BY " +  dimensionExpression;
        return query;
      }

      function all() {
        var query = writeQuery();
        // could use alias "key" here
        query += " ORDER BY " + dimensionExpression;
        return dataConnector.query(query);
      }

      function top(k) {
        var query = writeQuery();
        // could use alias "value" here
        query += " ORDER BY " + reduceExpression + " DESC LIMIT " + k;
        return dataConnector.query(query);
      }

      function bottom(k) {
        var query = writeQuery();
        // could use alias "value" here
        query += " ORDER BY " + reduceExpression;
        return dataConnector.query(query);
      }

      function reduceCount() {
        reduceExpression = "COUNT(*)";  
        return group;
      }

      function reduceSum(sumExpression) {
        reduceExpression = "SUM(" + sumExpression + ")";
        return group;
      }

      function reduceAvg(avgExpression) {
        reduceExpression = "AVG(" + avgExpression +")";  
        return group;
      }

      function size() {
        var query = "SELECT COUNT(DISTINCT(" + dimensionExpression + ")) AS n FROM " + dataTable;
        var filterQuery = writeFilter(); 
        if (filterQuery != "") {
          query += " WHERE " + filterQuery;
        }
        return dataConnector.query(query)[0]['n'];
      }

      return reduceCount();
    }

    function dispose() {
      filters.splice(dimensionIndex);
    }

    return dimension;
  }
  function groupAll() {
    var group = {
      reduceCount: reduceCount,
      reduceSum: reduceSum,
      reduceAvg: reduceAvg,
      value: value,
      dispose: dispose,
      remove: dispose // for backwards-compatibility
    };
    var reduceExpression = null; 
    
    function writeFilter() {
      var filterQuery = "";
      var nonNullFilterCount = 0;
      // we observe all filters
      for (var i = 0; i < filters.length ; i++) {
        if (filters[i] != "") {
          if (nonNullFilterCount > 0) {
            filterQuery += " AND ";
          }
          nonNullFilterCount++;
          filterQuery += filters[i];
        }
      }
      return filterQuery;
    }

    function writeQuery() {
      var query = "SELECT " + reduceExpression + " as value FROM " + dataTable ;
      var filterQuery = writeFilter(); 
      if (filterQuery != "") {
        query += " WHERE " + filterQuery;
      }
      // could use alias "key" here
      query += " GROUP BY " +  dimensionExpression;
      return query;
    }



    function reduceCount() {
      reduceExpression = "COUNT(*)";  
      return group;
    }

    function reduceSum(sumExpression) {
      reduceExpression = "SUM(" + sumExpression + ")";
      return group;
    }

    function reduceAvg(avgExpression) {
      reduceExpression = "AVG(" + avgExpression +")";  
      return group;
    }

    function value() {
      var query = writeQuery();
      // Below works because result set will be one field with one row
      return dataConnector.query(query)["results"][0][0];
    }
    return reduceCount();
  }


  // Returns the number of records in this crossfilter, irrespective of any filters.
  function size() {
    var query = "SELECT COUNT(*) as n FROM " + dataTable;
    return dataConnector.query(query)[0]['n'];
  }

  return (arguments.length == 2)
    ? setData(arguments[0],arguments[1]) // dataConnector, dataTable
    : crossfilter;

}
})(typeof exports !== 'undefined' && exports || this);
