/**
 * Created by andrelockhart on 5/6/17.
 */
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
// Dimension has no meaning outside CrossFilter, Group has no meaning
// outside Dimension
// todo - do we need a wrapper to orchestrate creation of objects and
// todo - provide a top level API e.g. create xfilter by passing in object
// todo - with xfilter, dimension, & group params, then exposing methods like
// todo - setDimension() to change dim for extant xfilter
// todo - this means we only need to store xfilter Ids and params in consumers
// todo - of xfilter lib vs storing entire xfilter objects
// todo - Also, should expose dc params as part of create API vs needing to
// todo - reference both xfilter and dc from consumer