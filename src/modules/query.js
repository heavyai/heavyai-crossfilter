export function formGroupSizeQuery(writeFilter, state, ignoreFilters) {
  var query = "SELECT "
  for (var d = 0; d < state.dimArray.length; d++) {
    if (d > 0) {
      query += ","
    }
    query += "APPROX_COUNT_DISTINCT(" + state.dimArray[d] + ") AS n"
    if (state.multiDim) {
      query += d.toString()
    }
  }
  query += " FROM " + state._tablesStmt
  if (!ignoreFilters) {
    // freeze bin state so they don"t change out from under us
    var queryBinParams = Array.isArray(state._binParams)
      ? [].concat(state._binParams)
      : []
    if (!queryBinParams.length) {
      queryBinParams = null
    }
    var filterQuery = writeFilter(queryBinParams)
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
