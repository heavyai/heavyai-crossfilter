export function formGroupSizeQuery(writeFilter, state, ignoreFilters) {
  let query = "SELECT "
  for (let d = 0; d < state._dimArray.length; d++) {
    if (d > 0) {
      query += ","
    }
    query += "COUNT(DISTINCT " + state._dimArray[d] + ") AS n"
    if (state.multiDim) {
      query += d.toString()
    }
  }
  query += " FROM " + state._tablesStmt
  if (!ignoreFilters) {

    // freeze bin state so they don"t change out from under us
    let queryBinParams = Array.isArray(state._binParams) ? [].concat(state._binParams) : []
    if (!queryBinParams.length) {
      queryBinParams = null
    }
    let filterQuery = writeFilter(queryBinParams)
    if (filterQuery != "") {
      query += " WHERE " + filterQuery
    }
    if (state._joinStmt !== null) {
      if (filterQuery === "") {
        query += " WHERE "
      } else {
        query += " AND "
      }
      query += state._joinStmt
    }
  } else {
    if (state._joinStmt !== null) {
      query += " WHERE " + state._joinStmt
    }
  }
  return query
}
