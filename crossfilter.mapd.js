(function(exports){
crossfilter.version = "1.3.11";

function crossfilter(dataDb, dataTable) {

  var crossfilter = {
    setData: setData, 
    dimension: dimension,
    groupAll: groupAll,
    size: size
  };

  var dataDb = null;
  var dataTable = null;
  var filters = [];
  
  function setData(newDataDb, newDataTable) {
    dataDb = newDataDb;
    dataTable = newDataTable;
    return crossfilter;
  }

  function dimension(value) {
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
    var exactFilter = null;
    var rangeFilter = null;
    var functionFilter = null;
    var filterType = null;

    var lo0 = 0,
        hi0 = 0;

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
      exactFilter = value;
      filterType = "exact";
      return dimension;
    }

    function filterRange(range) {
      rangeFilter = [range[0],range[1]];
      filterType = "range";
      return dimension;
    }

    function filterFunction(f) {
      filterFunction = f;
      filterType = "function";
      return dimension;
    }

    function filterAll() {
      filterType = null;
      return dimension;
    }

    function top(k) {



  }




