import { formGroupSizeQuery } from "../modules/query"
import { createQueryTask, runQueryTask } from "../modules/task"

/* istanbul ignore next */
export function mapResultToArray(queryResult, dimArrayAsArg) {
  return dimArrayAsArg.map((v, d) => {
    const varName = "n" + d.toString()
    return queryResult[varName]
  });
}

/* istanbul ignore next */
export function sizeAsyncWithEffects(queryTask, writeFilter) {
  return function sizeAsyncWithState(state, ignoreFilters, callback) {
    const query = formGroupSizeQuery(writeFilter, state, ignoreFilters),
     task = createQueryTask(queryTask, query)

      runQueryTask(task, (error, result) => {
          // console.log('sizeAsyncWithEffects.runQueryTask() - result: ', result)
        if (error) {
          callback(error)
        }
        else {
            if (state.multiDim) {
                const queryResult = result[0],
                    multiResult = mapResultToArray(queryResult, state.dimArray)
                callback(null, multiResult)
            }
            else {
                callback(null, result[0].n)
            }
        }
      })
    }
}

/* istanbul ignore next */
export function sizeSyncWithEffects(queryTask, writeFilter) {

  return function sizeSyncWithState(state, ignoreFilters) {
      // console.log('groupUtilities.sizeSyncWithState()')
      const query = formGroupSizeQuery(writeFilter, state, ignoreFilters),
          task = createQueryTask(queryTask, query)

    if (state.multiDim) {
        // console.log('groupUtilities.sizeSyncWithState() - has multiDim')
        const queryResult = runQueryTask(task)
        return mapResultToArray(queryResult, state.dimArray)
    }
    else {
        // console.log('groupUtilities.sizeSyncWithState() - no multiDim')
        const result = runQueryTask(task)
        return result[0].n
    }
  }
}
