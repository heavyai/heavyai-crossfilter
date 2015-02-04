(function(exports){
crossfilter.version = "1.3.11";

function crossfilter(dataConnector, dataDb, dataTable) {

  var crossfilter = {
    setData: setData, 
    dimension: dimension,
    groupAll: groupAll,
    size: size
  };

  var dataDb = null;
  var dataTable = null;
  var filters = [];
  
  function setData(newDataConnector, newDataDb, newDataTable) {
    dataConnector = newDataConnector;
    dataDb = newDataDb;
    dataTable = newDataTable;
    return crossfilter;
  }

  function dimension(expression) {
    var dimension = {
      filter: filter,
      filterExact: filterExact,
      filterRange: filterRange,
      filterAll: filterAll,
      top: top,
      group: group,
      groupAll: groupAll
      dispose: dispose
    };
    var index = filters.length;  
    filters.push(null);
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

    function filter(range) {
      return range == null
          ? filterAll() : Array.isArray(range)
          ? filterRange(range) : typeof range === "function"
          ? filterFunction(range)
          : filterExact(range);
    }

    function filterExact(value) {
      filters[index] = dimensionExpression + " = " + value; 
      return dimension;
    }

    function filterRange(range) {
      filters[index] = dimensionExpression + " >= " + range[0] + " AND " + dimensionExpression + " < " + range[1]; 
      return dimension;
    }

    function filterDisjunct(disjunctFilters) { // applying or with multiple filters"
      var lastFilterIndex = disjunctFilters.length - 1;
      for (var i = 0; i <= lastFilterIndex; i++) {
        var filter = disjunctFilters[i]; 
        if (Array.isArray(filter) {
          filters[index] += dimensionExpression + " >= " + filter[0] + " AND " + dimensionExpression + " < " + filter[1]; 
        }
        else {
          filters[index] += dimensionExpression + " = " + filter;
        }
        if (i != lastFilterIndex) {
          filters[index] += " OR ";
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
      filters[index] = null;
      return dimension;
    }

    // Returns the top K selected records based on this dimension's order.
    // Note: observes this dimension's filter, unlike group and groupAll.
    function writeDimensionQuery() {
      var query = "SELECT * FROM " + dataTable;
      var filterQuery = "";
      var nonNullFilterCount = 0;
      for (var i = 0; i < filters.length ; i++) {
        if (filters[i] != null) {
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
      var query = writeDimensionQuery();
      query += " ORDER BY " + dimensionExpression + " DESC LIMIT " + k; 
      return dataConnector.query(query);
    }

    function bottom(k) {
      var query = writeDimensionQuery();
      query += " ORDER BY " + dimensionExpression + " ASC LIMIT " + k; 
      return dataConnector.query(query);
    }






