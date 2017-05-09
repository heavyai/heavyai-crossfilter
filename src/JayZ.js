/**
 * Created by andrelockhart on 5/6/17.
 */
import CrossFilter from './CrossFilter'
import Dimension from './Dimension'
import Group from './group/Group'

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
export default function JayZ() {
    const crossfilter = new CrossFilter(),
          dimension   = new Dimension() // maybe this is instantiated by CrossFilter?
}


