import CrossFilter from './CrossFilter'
import ResultCache from './ResultCache'
import { filterNullMeasures, notEmpty, parseParensIfExist } from './group/Filter'

((exports) => {
    let crossfilterId = 0
    // const crossfilterSingleton = new CrossFilter()
    // crossfilter.version = "2.0.0" // todo - tisws
    exports.resultCache         = (con) => new ResultCache(con) // todo - this makes 0 sense AFAIK
    exports.crossfilter         = crossfilter
    exports.filterNullMeasures  = filterNullMeasures
    exports.notEmpty            = notEmpty
    exports.parseParensIfExist  = parseParensIfExist
    function crossfilter(dataConnector, dataTables, joinAttrs) {
        const crossFilter = new CrossFilter(crossfilterId++)
        return arguments.length >= 2 ? crossFilter.setDataAsync(dataConnector, dataTables, joinAttrs) : crossFilter
        // return crossFilter.setDataAsync(dataConnector, dataTables, joinAttrs)
    }
})(typeof exports !== "undefined" && exports || this)
