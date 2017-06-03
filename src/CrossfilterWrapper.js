import CrossFilter from "./CrossFilter"
import ResultCache from "./ResultCache"
import { filterNullMeasures, notEmpty, parseParensIfExist } from "./group/Filter"

((exports) => {
  let crossfilterId = 0
  exports.resultCache         = (con) => new ResultCache(con)
  exports.crossfilter         = crossfilter
  exports.filterNullMeasures  = filterNullMeasures
  exports.notEmpty            = notEmpty
  exports.parseParensIfExist  = parseParensIfExist
  function crossfilter(dataConnector, dataTables, joinAttrs) {
    const crossFilter = new CrossFilter(crossfilterId++)
    return arguments.length >= 2 ? crossFilter.setDataAsync(dataConnector, dataTables, joinAttrs) : crossFilter
  }
})(typeof exports !== "undefined" && exports || this)
