import { formGroupSizeQuery } from "./query"
import { createQueryTask, runQueryTask } from "./task"

/* istanbul ignore next */
export function mapResultToArray(queryResult, dimArrayAsArg) {
  return dimArrayAsArg.map(function(v, d) {
    var varName = "n" + d.toString()
    return queryResult[varName]
  })
}

/* istanbul ignore next */
export function sizeAsyncWithEffects(queryTask, writeFilter) {
  return function sizeAsyncWithState(state, ignoreFilters, callback) {
    var query = formGroupSizeQuery(writeFilter, state, ignoreFilters)
    var task = createQueryTask(queryTask, query)
    if (!state.multiDim) {
      runQueryTask(task, function(error, result) {
        if (error) {
          callback(error)
        } else {
          callback(null, result[0].n)
        }
      })
    } else {
      runQueryTask(task, function(error, result) {
        if (error) {
          callback(error)
        } else {
          var queryResult = result[0]
          var multiResult = mapResultToArray(queryResult, state.dimArray)
          callback(null, multiResult)
        }
      })
    }
  }
}

/* istanbul ignore next */
export function sizeSyncWithEffects(queryTask, writeFilter) {
  return function sizeSyncWithState(state, ignoreFilters) {
    var query = formGroupSizeQuery(writeFilter, state, ignoreFilters)
    var task = createQueryTask(queryTask, query)
    if (!state.multiDim) {
      var result = runQueryTask(task)
      return result[0].n
    } else {
      var queryResult = runQueryTask(task)
      return mapResultToArray(queryResult, state.dimArray)
    }
  }
}
