/**
 * Created by andrelockhart on 5/6/17.
 */
/**
 * A dimension is one or more columns to be queried. This is also used to
 * set ('global') filters on specific columns
 * multidimensional dimension has different behaviors to unidimensional dimension
 */
import ResultCache from '../ResultCache'
import Group from '../group/Group'
import { writeTopBottomQuery, writeTopQuery, top, writeBottomQuery, bottom } from './DimensionSQLWriter'
import { formatFilterValue } from '../group/Filter'

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

// todo - this is obviously a god class antipattern, filter is an obvious extraction
export default class Dimension {
    /******************************************************************
     * properties
     */
    type = 'dimension'
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
    // - means observe own filter and use conjunctive instead of disjunctive between sub-filters
    _drillDownFilter            = false
    _dimensionExpression        = null
    _samplingRatio              = null
    /***********   CONSTRUCTOR   ***************/
    // legacy params: expression, isGlobal
    constructor(dataConnector, crossfilter, expression, isGlobal) {
        this._init(dataConnector, crossfilter, expression, isGlobal)
        this.addPublicAPI()
    }
    /***********   INITIALIZATION   ***************/
    _init(dataConnector, crossfilter, expression, isGlobal) { // todo - initProps() initFunctions()
        this.getDataConnector = () => dataConnector

        // make crossfilter instance available to instance
        this.getCrossfilter = () => crossfilter
        /** set instance variables **/
        this._cache             = new ResultCache(dataConnector)
        // todo - this index is used to access crossfilter dimensions and filters arrays to null on dispose() & remove()
        // new dimensions are tacked into the end of the dimension array
        this._dimensionIndex    = isGlobal ? crossfilter._globalFilters.length : crossfilter._filters.length
        this._scopedFilters     = isGlobal ? crossfilter._globalFilters : crossfilter._filters
        this._scopedFilters.push('')
        this._expression = Array.isArray(this._expression) ? this._expression : [this._expression] // todo - fix
        this._isMultiDim = expression.length > 1
        this._columns    = _mapColumnsToNameAndType(crossfilter.getColumns())
        // this the collection of columns, expressed as strings
        // can also cast
        this._initDimArray(expression, crossfilter)
        this._initDimContainsArray(crossfilter)
        this._initGroup()
    }
    _initDimArray(expression) {
        this._dimArray = expression.map((field) => {
            let indexOfColumn   = _findIndexOfColumn(this._columns, field),
                isDate          = indexOfColumn > -1 && _isDateField(this._columns[indexOfColumn])
            if (isDate) {
                field = "CAST(" + field + " AS TIMESTAMP(0))"
            }
            return field
        })
        this._dimensionExpression = this._dimArray.includes(null) ? null : this._dimArray.join(", ")
    }
    _initDimContainsArray(crossfilter) {
        const { _dimArray, _dimContainsArray }      = this,
            { _columnTypeMap, _compoundColumnMap }  = crossfilter

        _dimArray.forEach((dim, i) => {
            if (dim in _columnTypeMap) {
                _dimContainsArray[i] = _columnTypeMap[dim.is_array]
            }
            else if (dim in _compoundColumnMap) {
                _dimContainsArray[i] = _columnTypeMap[_compoundColumnMap[dim].is_array]
            }
            else {
                _dimContainsArray[i] = false
            }
        })
    }
    _initGroup(dataConnector) {
        // tbd
    }
    // todo - maybe this or some other technique...?
    addPublicAPI() {
        this.writeTopBottomQuery = writeTopBottomQuery
        this.writeTopQuery = writeTopQuery
        this.top = top
        this.writeBottomQuery = writeBottomQuery
        this.bottom = bottom
        // todo - temporary hack to support backwards compatibility
        this.remove = this.dispose = () => this.getCrossfilter().removeDimension(this)
    }
    /******************************************************************
     * private methods
     */
    /******************************************************************
     * public methods
     */
    // todo - tricky: look at immerse/src/services/crossfilter.getTopN
    group(groupId = false) { // todo - shouldn't this at least be parameterized like thus?
        if(!this._dimensionGroups.length) {
            const newGroup = new Group(this.getDataConnector(), this)
            return this.addGroupToDimension(newGroup)
        }
        else {
            // todo - such an excellent question, eh? (tisws)
        }
    }
    addGroupToDimension(newGroup) {
        this._dimensionGroups.push(newGroup)
        return newGroup
    }
    /**
     *  tbd public or private methods
     */
    multiDim (value) {
        if (typeof value === "boolean") {
            this._isMultiDim = value
            return this
        }
        return this._isMultiDim
    }
    // todo - make this param consistent with immerse/src/services/crossfilter.getTopN(), which passes in 'column'
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
        return this // todo - returning 'this' is inconsistent
    }
    toggleTarget(crossfilter) {
        let { _targetFilter } = crossfilter
        if (_targetFilter === this._dimensionIndex) { // TODO duplicates isTargeting
            _targetFilter = null // TODO duplicates removeTarget
        } else {
            _targetFilter = this._dimensionIndex
        }
    }
    removeTarget(crossfilter) {
        if (crossfilter._targetFilter === this._dimensionIndex) {
            crossfilter._targetFilter = null
        }
    }
    isTargeting(crossfilter) {
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
    /** filter methods **/
    selfFilter(_) { // todo - '_' is used to set _selfFilter, so it should have a semantic name!!!
        if (!arguments.length)
            return this._selfFilter
        this._selfFilter = _
        return this // todo - returning 'this' is inconsistent
    }
    getFilter = () => this._filterVal
    getFilterString() {
        return this._scopedFilters[this._dimensionIndex]
    }
    filter(range, append = false, resetRange, inverseFilter, binParams = [{extract: false}]) {
        if (typeof range === 'undefined') {
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
        let { _scopedFilters, _dimensionIndex, _dimArray, _dimContainsArray } = this,
            subExpression = ""

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
            _scopedFilters[_dimensionIndex] += subExpression
        } else {
            _scopedFilters[_dimensionIndex] = subExpression
        }
        return this
    }
    formNotEqualsExpression(value) {
        let escaped = formatFilterValue(value, true, true)
        return this._dimensionExpression + " <> " + escaped
    }
    filterNotEquals(value, append) {
        let { _scopedFilters, _dimensionIndex } = this
        if (append) {
            _scopedFilters[_dimensionIndex] += this.formNotEqualsExpression(value)
        } else {
            _scopedFilters[_dimensionIndex] = this.formNotEqualsExpression(value)
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
        let { _scopedFilters, _dimensionIndex } = this
        if (append) {
            _scopedFilters[_dimensionIndex] += this.formLikeExpression(value)
        } else {
            _scopedFilters[_dimensionIndex] = this.formLikeExpression(value)
        }
        return this
    }
    filterILike(value, append) {
        let { _scopedFilters, _dimensionIndex } = this
        if (append) {
            _scopedFilters[_dimensionIndex] += this.formILikeExpression(value)
        } else {
            _scopedFilters[_dimensionIndex] = this.formILikeExpression(value)
        }
        return this
    }
    // todo - make filter functions DRY
    filterNotLike(value, append) {
        let { _scopedFilters, _dimensionIndex } = this
        if (append) {
            _scopedFilters[_dimensionIndex] += "NOT( " + this.formLikeExpression(value) + ")"
        } else {
            _scopedFilters[_dimensionIndex] = "NOT( " + this.formLikeExpression(value) + ")"
        }
        return this
    }
    filterNotILike(value, append) {
        let { _scopedFilters, _dimensionIndex } = this
        if (append) {
            _scopedFilters[_dimensionIndex] += "NOT( " + this.formILikeExpression(value) + ")"
        } else {
            _scopedFilters[_dimensionIndex] = "NOT( " + this.formILikeExpression(value) + ")"
        }
        return this
    }
    filterIsNotNull(append) {
        let { _scopedFilters, _dimensionIndex } = this
        if (append) {
            _scopedFilters[_dimensionIndex] += `${expression} IS NOT NULL`
        } else {
            _scopedFilters[_dimensionIndex] = `${expression} IS NOT NULL`
        }
        return this
    }
    filterIsNull(append) {
        let { _scopedFilters, _dimensionIndex } = this
        if (append) {
            _scopedFilters[_dimensionIndex] += `${expression} IS NULL`
        } else {
            _scopedFilters[_dimensionIndex] = `${expression} IS NULL`
        }
        return this
    }
    filterRange(range, append = false, resetRange, inverseFilters, binParams, isRelative) {
        let { _filterVal, _rangeFilters, _dimensionIndex, _scopedFilters, _dimArray, _drillDownFilter } = this,
            isArray       = Array.isArray(range[0]), // TODO semi-risky index
            subExpression = ""

        if (!isArray) {
            range = [range]
        }
        _filterVal = range

        for (let e = 0; e < range.length; e++) {
            if (resetRange === true) {
                _rangeFilters[e] = range[e]
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
            _scopedFilters[_dimensionIndex] += "(" + subExpression + ")"
        } else {
            _scopedFilters[_dimensionIndex] = "(" + subExpression + ")"
        }
        return this
    }
    formatRelativeValue(val) {
        if (val.now) {
            return "NOW()" // todo - there might be subtle bugs in Now(), depending on when it's called & evaluated
        } else if (val.datepart && typeof val.number !== 'undefined') {
            const date      = typeof val.date !== 'undefined' ? val.date : "NOW()",
                operator    = typeof val.operator !== 'undefined' ? val.operator : "DATE_ADD",
                number      = isNaN(val.number) ? this.formatRelativeValue(val.number) : val.number,
                add         = typeof val.add !== 'undefined' ? val.add : ""
            return `${operator}(${val.datepart}, ${number}, ${date})${add}`
        } else {
            return val
        }
    }
    filterMulti(filterArray, resetRangeIn, inverseFilters, binParams) {
        let { _dimensionIndex, _scopedFilters, _drillDownFilter } = this,
            { _filters }                        = this.getCrossfilter(),
            resetRange                          = false

        if (resetRangeIn !== undefined) {
            resetRange = resetRangeIn
        }

        let lastFilterIndex = filterArray.length - 1
        _scopedFilters[_dimensionIndex] = "("

        inverseFilters = typeof (inverseFilters) === "undefined" ? false : inverseFilters

        filterArray.forEach((currentFilter) => {
            filter(currentFilter, true, resetRange, inverseFilters, binParams)
            if (i !== lastFilterIndex) {
                if (_drillDownFilter ^ inverseFilters) {
                    _filters[_dimensionIndex] += " AND "
                } else {
                    _filters[_dimensionIndex] += " OR "
                }
            }
        })
        _scopedFilters[_dimensionIndex] += ")"
        return this
    }
    filterAll(softFilterClear) {
        let { _rangeFilters, _filterVal, _scopedFilters, _dimensionIndex } = this
        if (softFilterClear === undefined || softFilterClear === false) {
            _rangeFilters = []
        }
        _filterVal = null
        _scopedFilters[_dimensionIndex] = ""
        return this
    }
    samplingRatio(ratio) {
        if (!ratio)
            this._samplingRatio = null
        this._samplingRatio = ratio // TODO always overwrites; typo?
        return this
    }
    writeTopBottomQuery(k, offset, ascDescExpr, isRender) {
        const { _orderExpression, _dimensionExpression } = this
        let query = writeQuery(!!isRender)
        if (!query) {
            return ''
        }
        if (_orderExpression) { // overrides any other ordering based on dimension
            query += " ORDER BY " + _orderExpression + ascDescExpr
        } else if (_dimensionExpression)  {
            query += " ORDER BY " + _dimensionExpression + ascDescExpr
        }
        if (k !== Infinity) {
            query += " LIMIT " + k
        }
        if (offset !== undefined) {
            query += " OFFSET " + offset
        }
        return query
    }
}