export function createQueryTask(method, query, options) {
  return function(callback) {
    return method(query, options, callback)
  }
}

export function runQueryTask(task, callback) {
  if (callback) {
    task(callback)
  } else {
    try {
      var result = task()
      return result
    } catch (e) {
      throw e
    }
  }
}
