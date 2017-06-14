/**
 * A dimension is one or more columns to be queried. This is also used to
 * set ('global') filters on specific columns
 * multidimensional dimension has different behaviors to unidimensional dimension
 */
import ResultCache from "../ResultCache"
import Group from "../group/Group"
import { top, topAsync, writeTopQuery, writeTopBottomQuery, bottom, writeBottomQuery } from "./DimensionSQLWriter"
import { formatFilterValue } from "../group/Filter"

function _isDateField(field) { return field.type === "DATE" }
function _mapColumnsToNameAndType(columns) {
  return Object.keys(columns).map(function (key) {
    let col = columns[key]
    return { rawColumn: key, column: col.column, type: col.type }
  })
}
function _findIndexOfColumn(columns, targetColumn) {
  return columns.reduce(function (colIndex, col, i) {
    let containsField = col.rawColumn === targetColumn || col.column === targetColumn
    if (colIndex === -1 && containsField) { colIndex = i }
    return colIndex
  }, -1)
}
function uncast (string) {
  const matching = string.match(/^CAST\([a-z,_]{0,250}/)
  if (matching) {
    return matching[0].split("CAST(")[1]
  } else {
    return string
  }
}

export default class Dimension {
  /******************************************************************
   * properties
   */
  type = "dimension"
  _dimensionIndex             = null
  _filterVal                  = null
  _allowTargeted              = true
  _selfFilter                 = null
  _dimensionGroups            = []
  _orderExpression            = null
  _projectExpressions         = []
  _projectOnAllDimensionsFlag = false
  _rangeFilters               = []
  _dimContainsArray           = []
  _eliminateNull              = true
  // option for array columns
  // - means observe own filter and use conjunctive
  // instead of disjunctive between sub-filters
  _drillDownFilter            = false
  _dimensionExpression        = null
  _samplingRatio              = null
  /***********   CONSTRUCTOR   ***************/
  // legacy params: expression, isGlobal
  constructor(crossfilter, expression = false, isGlobal) {
    this._init(crossfilter, expression, isGlobal)
    this._addPublicAPI(crossfilter)
  }
  /***********   INITIALIZATION   ***************/
  _init(crossfilter, expression, isGlobal) {
    this.getDataConnector = () => crossfilter._dataConnector
    // makes crossfilter instance available to instance
    this.getCrossfilter = () => crossfilter
    /** set instance variables **/
    this._cache             = new ResultCache(crossfilter._dataConnector)
    // this index is used to access crossfilter dimensions and filters arrays to null on dispose() & remove()
    // new dimensions are pushed into the dimension array
    this._dimensionIndex    = isGlobal ? crossfilter._globalFilters.length : crossfilter._filters.length
    this._scopedFilters     = isGlobal ? crossfilter._globalFilters : crossfilter._filters
    this._scopedFilters.push("")
    this._expression = Array.isArray(expression) ? expression : [expression]
    this._isMultiDim = this._expression.length > 1
    this._columns    = _mapColumnsToNameAndType(crossfilter.getColumns())
    // this the collection of columns, expressed as strings, can also cast
    this._initDimArray(this._expression, crossfilter)
    this._initDimContainsArray(crossfilter)
  }
  _initDimArray(expression) {
    this._dimArray = expression.map((field) => {
      let indexOfColumn   = _findIndexOfColumn(this._columns, field),
        isDate          = indexOfColumn > - 1 && _isDateField(this._columns[indexOfColumn])
      if (isDate) {
        field = `CAST(${field} AS TIMESTAMP(0))`
      }
      return field
    })
    this._dimensionExpression = this._dimArray.includes(null) ? null : this._dimArray.join(", ")
  }
  _initDimContainsArray(crossfilter) {
    const { _dimArray }      = this,
      { _columnTypeMap, _compoundColumnMap }  = crossfilter

    _dimArray.forEach((dim, i) => {
      if (dim in _columnTypeMap) {
        this._dimContainsArray[i] = _columnTypeMap[dim].is_array
      }
      else if (dim in _compoundColumnMap) {
        this._dimContainsArray[i] = _columnTypeMap[_compoundColumnMap[dim]].is_array
      }
      else {
        this._dimContainsArray[i] = false
      }
    })
  }
  _addPublicAPI(crossfilter) {
    this.writeTopBottomQuery    = writeTopBottomQuery
    this.top                    = (k, offset, renderSpec, callback) => top(this, k, offset, renderSpec, callback)
    this.topAsync               = (k, offset, renderSpec, callback) => topAsync(this, k, offset, renderSpec)
    this.writeTopQuery          = (k, offset, isRender) => writeTopQuery(this, k, offset, isRender)
    this.writeTopBottomQuery    = (k, offset, ascDescExpr, isRender) => writeTopBottomQuery(this, k, ascDescExpr, isRender)
    this.bottom                 = (k, offset, renderSpec, callback) => bottom(this, k, offset, renderSpec, callback)
    this.bottomAsync            = this.bottom
    this.writeBottomQuery       = (k, offset, isRender) => writeBottomQuery(this, k, offset, isRender)
    // support backwards compatibility
    this.remove = this.dispose  = () => this.getCrossfilter().removeDimension(this)
    this.value                  = () => this._dimArray
    this.getCrossfilterId       = () => crossfilter.getId()
    this.getProjectOn           = () => this._projectExpressions
    this.setDrillDownFilter     = (value) => {
      this._drillDownFilter = value
      return this
    }
    this.getTable               = crossfilter.getTable
    // for unit testing only
    this.getSamplingRatio       = () => this._samplingRatio
    this.clearCache = () => {
      this._cache = new ResultCache(crossfilter._dataConnector)
      this._dimensionGroups.forEach((dimensionGroup) => {
        dimensionGroup.clearCache()
      })
    }
  }
  set(fn) {
    this._dimArray = fn(this._dimArray)
    return this
  }
  /******************************************************************
   * private methods
   */
  /******************************************************************
   * public methods
   */
  group() {
    const newGroup = new Group(this.getDataConnector(), this)
    return this.addGroupToDimension(newGroup)
  }
  addGroupToDimension(newGroup) {
    this._dimensionGroups.push(newGroup)
    return newGroup
  }
  groupAll() {
    return this.getCrossfilter().groupAll
  }
  setEliminateNull(eliminateNull) {
    this._eliminateNull = eliminateNull
    return this
  }
  getEliminateNull() {
    return this._eliminateNull
  }
  multiDim (value) {
    if (typeof value === "boolean") {
      this._isMultiDim = value
      return this
    }
    return this._isMultiDim
  }
  order(orderExpression) {
    this._orderExpression = orderExpression
    return this
  }
  orderNatural() {
    this._orderExpression = null
    return this
  }
  allowTargeted(allowTargeted) {
    if (!arguments.length) {
      return this._allowTargeted
    }
    this._allowTargeted = allowTargeted
    return this
  }
  toggleTarget() {
    const crossfilter = this.getCrossfilter()
    if (crossfilter._targetFilter == this._dimensionIndex) {
      crossfilter._targetFilter = null
    } else {
      crossfilter._targetFilter = this._dimensionIndex
    }
  }
  removeTarget() {
    const crossfilter = this.getCrossfilter()
    if (crossfilter._targetFilter == this._dimensionIndex) {
      crossfilter._targetFilter = null
    }
  }
  isTargeting() {
    const crossfilter = this.getCrossfilter()
    return crossfilter._targetFilter === this._dimensionIndex
  }
  projectOn(expressions) {
    this._projectExpressions = expressions
    return this
  }
  projectOnAllDimensions(flag) {
    this._projectOnAllDimensionsFlag = flag
    return this
  }
  /******************************************************************
   * filtering
   */
  selfFilter(filterString) {
    if (!arguments.length)
      return this._selfFilter
    this._selfFilter = filterString
    return this
  }
  getFilter() {
    return this._filterVal
  }
  getFilterString() {
    return this._scopedFilters[this._dimensionIndex]
  }
  filter(range, append = false, resetRange, inverseFilter, binParams = [{extract: false}]) {
    if (typeof range == "undefined") {
      return this.filterAll()
    } else if (Array.isArray(range) && !this._isMultiDim) {
      return this.filterRange(range, append, resetRange, inverseFilter, binParams)
    } else {
      return this.filterExact(range, append, inverseFilter, binParams)
    }
  }
  filterRelative(range, append = false, resetRange, inverseFilter) {
    return this.filterRange(range, append, resetRange, inverseFilter, null, true)
  }
  filterExact(value, append, inverseFilter, binParams = []) {
    const { _dimensionIndex, _dimArray, _dimContainsArray } = this
    let subExpression = ""
    value = Array.isArray(value) ? value : [value]
    for (let e = 0; e < value.length; e++) {
      if (e > 0) {
        subExpression += " AND "
      }
      let typedValue = formatFilterValue(value[e], true, true)
      if (_dimContainsArray[e]) {
        subExpression += typedValue + " = ANY " + _dimArray[e]
      }
      else if (Array.isArray(typedValue)) {
        if (typedValue[0] instanceof Date) {
          const min       = formatFilterValue(typedValue[0]),
            max         = formatFilterValue(typedValue[1]),
            dimension   = _dimArray[e]

          subExpression += dimension + " >= " + min + " AND " + dimension + " <= " + max
        }
        else {
          const min       = typedValue[0],
            max         = typedValue[1],
            dimension   = _dimArray[e]

          subExpression += dimension + " >= " + min + " AND " + dimension + " <= " + max
        }
      } else {
        if (binParams[e] && binParams[e].extract) {
          subExpression += "extract(" + binParams[e].timeBin + " from " + uncast(_dimArray[e]) +  ") = " + typedValue
        } else {
          subExpression += typedValue === null ? `${_dimArray[e]} IS NULL` : `${_dimArray[e]} = ${typedValue}`
        }
      }
    }
    if (inverseFilter) {
      subExpression = "NOT (" + subExpression + ")"
    }
    if (append) {
      this._scopedFilters[_dimensionIndex] += subExpression
    } else {
      this._scopedFilters[_dimensionIndex] = subExpression
    }
    return this
  }
  formNotEqualsExpression(value) {
    let escaped = formatFilterValue(value, true, true)
    return this._dimensionExpression + " <> " + escaped
  }
  filterNotEquals(value, append) {
    const { _dimensionIndex } = this
    if (append) {
      this._scopedFilters[_dimensionIndex] += this.formNotEqualsExpression(value)
    } else {
      this._scopedFilters[_dimensionIndex] = this.formNotEqualsExpression(value)
    }
    return this
  }
  formLikeExpression(value) {
    let escaped = formatFilterValue(value, false, false)
    return this._dimensionExpression + " like '%" + escaped + "%'"
  }
  formILikeExpression(value) {
    let escaped = formatFilterValue(value, false, false)
    return this._dimensionExpression + " ilike '%" + escaped + "%'"
  }
  filterLike(value, append) {
    const { _dimensionIndex } = this
    if (append) {
      this._scopedFilters[_dimensionIndex] += this.formLikeExpression(value)
    } else {
      this._scopedFilters[_dimensionIndex] = this.formLikeExpression(value)
    }
    return this
  }
  filterILike(value, append) {
    const { _dimensionIndex } = this
    if (append) {
      this._scopedFilters[_dimensionIndex] += this.formILikeExpression(value)
    } else {
      this._scopedFilters[_dimensionIndex] = this.formILikeExpression(value)
    }
    return this
  }
  filterNotLike(value, append) {
    const { _dimensionIndex } = this
    if (append) {
      this._scopedFilters[_dimensionIndex] += "NOT( " + this.formLikeExpression(value) + ")"
    } else {
      this._scopedFilters[_dimensionIndex] = "NOT( " + this.formLikeExpression(value) + ")"
    }
    return this
  }
  filterNotILike(value, append) {
    const { _dimensionIndex } = this
    if (append) {
      this._scopedFilters[_dimensionIndex] += "NOT( " + this.formILikeExpression(value) + ")"
    } else {
      this._scopedFilters[_dimensionIndex] = "NOT( " + this.formILikeExpression(value) + ")"
    }
    return this
  }
  filterIsNotNull(append) {
    const { _dimensionIndex, _expression } = this
    if (append) {
      this._scopedFilters[_dimensionIndex] += `${_expression} IS NOT NULL`
    } else {
      this._scopedFilters[_dimensionIndex] = `${_expression} IS NOT NULL`
    }
    return this
  }
  filterIsNull(append) {
    const { _dimensionIndex, _expression } = this
    if (append) {
      this._scopedFilters[_dimensionIndex] += `${_expression} IS NULL`
    } else {
      this._scopedFilters[_dimensionIndex] = `${_expression} IS NULL`
    }
    return this
  }
  filterRange(range, append = false, resetRange, inverseFilters, binParams, isRelative) {
    const { _dimensionIndex, _dimArray } = this,
                                isArray  = Array.isArray(range[0])
    let subExpression = ""

    if (!isArray) {
      range = [range]
    }
    this._filterVal = range

    for (let e = 0; e < range.length; e++) {
      if (resetRange === true) {
        this._rangeFilters[e] = range[e]
      }
      if (e > 0) {
        subExpression += " AND "
      }

      let typedRange = [
        formatFilterValue(range[e][0], true),
        formatFilterValue(range[e][1], true)
      ]
      if (isRelative) {
        typedRange = [
          this.formatRelativeValue(typedRange[0]),
          this.formatRelativeValue(typedRange[1])
        ]
      }
      if (binParams && binParams[e] && binParams[e].extract) {
        const dimension = "extract(" + binParams[e].timeBin + " from " + uncast(_dimArray[e]) +  ")"
        subExpression += dimension + " >= " + typedRange[0] + " AND " + dimension + " <= " + typedRange[1]
      } else {
        subExpression += _dimArray[e] + " >= " + typedRange[0] + " AND " + _dimArray[e] + " <= " + typedRange[1]
      }
    }
    if (inverseFilters) {
      subExpression = "NOT(" + subExpression + ")"
    }
    if (append) {
      this._scopedFilters[_dimensionIndex] += "(" + subExpression + ")"
    } else {
      this._scopedFilters[_dimensionIndex] = "(" + subExpression + ")"
    }
    return this
  }
  formatRelativeValue(val) {
    if (val.now) {
      return "NOW()"
    } else if (val.datepart && typeof val.number !== "undefined") {
      const date      = typeof val.date !== "undefined" ? val.date : "NOW()",
        operator    = typeof val.operator !== "undefined" ? val.operator : "DATE_ADD",
        number      = isNaN(val.number) ? this.formatRelativeValue(val.number) : val.number,
        add         = typeof val.add !== "undefined" ? val.add : ""
      return `${operator}(${val.datepart}, ${number}, ${date})${add}`
    } else {
      return val
    }
  }
  filterMulti(filterArray, resetRangeIn, inverseFilters, binParams) {
    const { _dimensionIndex, _drillDownFilter } = this,
      crossfilter                             = this.getCrossfilter()
    let resetRange = false
    if (resetRangeIn !== undefined) {
      resetRange = resetRangeIn
    }
    const lastFilterIndex = filterArray.length - 1
    this._scopedFilters[_dimensionIndex] = "("

    inverseFilters = typeof (inverseFilters) === "undefined" ? false : inverseFilters

    filterArray.forEach((currentFilter, i) => {
      this.filter(currentFilter, true, resetRange, inverseFilters, binParams)
      if (i !== lastFilterIndex) {
        if (_drillDownFilter ^ inverseFilters) {
          crossfilter._filters[_dimensionIndex] += " AND "
        } else {
          crossfilter._filters[_dimensionIndex] += " OR "
        }
      }
    })
    this._scopedFilters[_dimensionIndex] += ")"
    return this
  }
  filterAll(softFilterClear) {
    const { _dimensionIndex } = this
    if (softFilterClear === undefined || softFilterClear === false) {
      this._rangeFilters = []
    }
    this._filterVal = null
    this._scopedFilters[_dimensionIndex] = ""
    return this
  }
  samplingRatio(ratio) {
    if (!ratio)
      this._samplingRatio = null
    this._samplingRatio = ratio
    return this
  }
}
