export function createQueryTask(method, query, options) {
    // console.log('task.createQueryTask() - value of arguments: ', arguments)
  return function (callback) {
    return method(query, options, callback)
  }
}

export function runQueryTask(task, callback) {
  if (callback) {
    task(callback);
  } else {
    try {
      return task()
    } catch (e) {
      throw e
    }
  }
}
