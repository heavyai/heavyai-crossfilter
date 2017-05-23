/**
 * Created by andrelockhart on 5/6/17.
 */
'use strict'
import ResultCache from './ResultCache'
import Dimension from './dimension/Dimension'
import GroupAll from './group/GroupAll'
/**
 *  Marries connector context with state context. Holds knowledge of one set of tables and all
 *  filtering and organization (group, bin) of it
 */
// let instance = null
export default class CrossFilter {
    /******************************************************************
     * properties
     */
    type = 'crossfilter' // todo - as GW said, 'this is some weird shit' (tisws)
    // todo - I'm not a fan of hard coding versions in the source e.g. crossfilter.version = '1.3.11'
    // todo - is the intent to make these private members? if so, use Crockford constructor trick
    // _dataTables          = null
    // FROM clause: assumes all groups and dimensions of this xfilter instance use collection of columns
    // it is most apparent in a multi-layer pointmap (e.g. tweets and contributions ala example)
    _targetFilter        = null
    // todo - replace dimensions and filters implementation with simplified approach that enables diff/union of sets, maybe using uuid for key
    _dimensions          = [] // TODO: muy importante - should this be stored here?
    _filters             = []
    _globalFilters       = []
    _dataTables          = []
    _tablesStmt          = null
    _id                  = null
    /***********   CONSTRUCTOR   ***************/
    constructor(crossfilterId) {
        // singleton enforcer
        // if(instance) {
        //     return instance
        // }
        // else {
        //     instance = this
        //     this._init()
        // }
        this._init(crossfilterId)
        this._addPublicAPI()
    }
    _init(crossfilterId) {
        this._id = crossfilterId
    }
    /******************************************************************
     * private methods
     */
    _addPublicAPI() {
        this.getId = () => this._id
        this.getDataTables = () => this._dataTables
        this.getTable = () => this._dataTables
    }
    initCrossFilterForAsync(dataConnector, dataTables) {
        this._dataConnector      = dataConnector
        this._cache              = new ResultCache(dataConnector) // this should gc old cache...
        this._dataTables         = dataTables
        this._columnTypeMap      = {}
        this._compoundColumnMap  = {}
        this._joinAttrMap        = {}
        this._joinStmt           = null
        this._tablesStmt         = ''
    }
    /**
     * manage dimensions
     */
    // backwards compatibility
    dimension(expression, isGlobal = false) {
        const { _dataConnector } = this
        const newDimension = new Dimension(this, expression, isGlobal)
        return this.addDimension(newDimension)
    }
    addDimension(newDimension) {
        newDimension._dimensionIndex = (this._dimensions.push(newDimension) -1)
        return newDimension
    }
    // todo - do dim/grp need to de-reference cf/dim
    // todo - prior to removal so they're gc'ed?
    // todo - what will call this?
    removeDimension(dimensionIndex) {
        // todo - need to ensure gc
        // todo - one object should encapsulate all reference mgmt
        // todo - what about global filters?
        // todo - stop array from becoming sparse, and prevent leaks
        this._dimensions.splice(dimensionIndex, 1)
        this._filters.splice(dimensionIndex, 1)
    }
    // todo - old dispose/remove method
    // function dispose() {
    // todo - why are we keeping empty stuff in array?
    //     filters[dimensionIndex] = null;
    //     dimensions[dimensionIndex] = null;
    // }
    /**
     * manage GroupAll
     */
    groupAll() {
        return new GroupAll(this._dataConnector, this)
    }
    /**
     * manage cache
     */
    clearAllCaches() {
        this._dimensions    = []
        this._filters       = []
        this._globalFilters = []
    }
    /******************************************************************
     * public methods
     */
    setDataAsync(dataConnector, dataTables, joinAttrs) {
        // joinAttrs should be an array of objects with keys
        // table1, table2, attr1, attr2
        this.initCrossFilterForAsync(dataConnector, dataTables)
        if (!Array.isArray(this._dataTables)) {
            this._dataTables = [this._dataTables]
        }
        this._dataTables.forEach((table, i) => {
            if (i > 0) {
                this._tablesStmt += ","
            }
            this._tablesStmt += table
        })
        if (typeof joinAttrs !== 'undefined') {
            this._joinStmt = ''
            console.log('Crossfilter.setDataAsync - value of joinAttrs: ', joinAttrs)
            // todo - tisws: this smells brittle and hard coded (!important for first refactoring)
            joinAttrs.forEach((join, i) => {
                const joinKey = join.table1 < join.table2 ?
                    join.table1 + "." + join.table2 : join.table2 + "." + join.table1,
                 tableJoinStmt = join.table1 + "." + join.attr1 + " = "
                    + join.table2 + "." + join.attr2
                if (i > 0) {
                    this._joinStmt += " AND "
                }
                this._joinStmt += tableJoinStmt
                this._joinAttrMap[joinKey] = tableJoinStmt
            })
        }
        console.log('Crossfilter.setDataAsync - value of _joinStmt: ', this._joinStmt)
        return Promise.all(this._dataTables.map(this.getFieldsPromise.bind(this)))
            .then(() => this)
    }
    getFieldsPromise(table) {
        return new Promise((resolve, reject) => {
            // debugger
            this._dataConnector.getFields(table, (error, columnsArray) => {
                if (error) {
                    reject(error)
                } else {
                    let columnNameCountMap = {}
                    columnsArray.forEach((element) => {
                        let compoundName = table + "." + element.name
                        this._columnTypeMap[compoundName] = {
                            table               : table,
                            column              : element.name,
                            type                : element.type,
                            is_array            : element.is_array,
                            is_dict             : element.is_dict,
                            name_is_ambiguous   : false
                        }
                        columnNameCountMap[element.name] = columnNameCountMap[element.name] === undefined ?
                            1 : columnNameCountMap[element.name] + 1
                    })
                    Reflect.ownKeys(this._columnTypeMap).map((key) => {
                        if (columnNameCountMap[this._columnTypeMap[key].column] > 1) {
                            this._columnTypeMap[key].name_is_ambiguous = true
                        } else {
                            this._compoundColumnMap[this._columnTypeMap[key].column] = key
                        }
                    })
                    resolve(this)
                }
            })
        })
    }
    getColumns() {
        return this._columnTypeMap
    }
    /** filtering **/
    getFilterString() {
        let filterString = ""
        this._filters.forEach((value, i) => {
            if (value !== null && value !== "") {
                if (i) {
                    filterString += " AND "
                }
                filterString += value
            }
        })
        return filterString
    }
    getGlobalFilterString() {
        let filterString = ""
        this._globalFilters.forEach((value, i) => {
            if (value !== null && value !== "") {
                if (i) {
                    filterString += " AND "
                }
                filterString += value
            }
        })
        return filterString
    }
    filter(isGlobal) {
        let me = this,
            { _globalFilters, _filters, _targetFilter } = this,
            filterIndex
        var filter = {
            filter          : filter,
            filterAll       : filterAll,
            getFilter       : getFilter,
            toggleTarget    : toggleTarget,
            getTargetFilter : () => this._targetFilter
        }
        if (isGlobal) {
            console.log('global filters %%%%%%%%%%%% %%%%%%%%%%%%%%')
            filterIndex = _globalFilters.length
            this._globalFilters.push("")
        } else {
            filterIndex = _filters.length
            this._filters.push("")
        }
        function toggleTarget() {
            this._targetFilter === filterIndex ? _targetFilter = null : _targetFilter = filterIndex
            return filter
        }
        function getFilter() {
            console.log('Crossfilter.filter - getFilter() value of filter: ', isGlobal ? _globalFilters[filterIndex] : _filters[filterIndex])
            return isGlobal ? _globalFilters[filterIndex] : _filters[filterIndex]
        }
        function filter(filterExpr) {
            console.log('Crossfilter.filter - inner filter(), value of filterExpr', filterExpr)
            // debugger
            if (filterExpr === undefined || filterExpr ===  null) {
                filterAll()
            } else if (isGlobal) {
                me._globalFilters[filterIndex] = filterExpr
            } else {
                debugger
                me._filters[filterIndex] = filterExpr
            }
            return filter
        }
        function filterAll() {
            console.log('Crossfilter.filter - filterAll()')
            isGlobal ? this._globalFilters[filterIndex] = "" : this._filters[filterIndex] = ""
            return filter
        }
        return filter
    }
    // Returns the number of records in this crossfilter, irrespective of any filters.
    size(callback) {
        const { _tablesStmt, _joinStmt } = this
        // debugger
        if (!callback) {
            console.warn("Warning: Deprecated sync method groupAll.size(). Please use async version")
        }
        let query = "SELECT COUNT(*) as n FROM " + _tablesStmt
        if (_joinStmt !== null) {
            query += " WHERE " + _joinStmt
        }

        const options = {
            eliminateNullRows   : false,
            renderSpec          : null,
            postProcessors      : [(d) => d[0].n]
        }
        if (callback) {
            return this._cache.queryAsync(query, options, callback)
        } else {
            return this._cache.query(query, options)
        }
    }
    sizeAsync() {
        return new Promise((resolve, reject) => {
            this.size((error, data) => {
                if (error) {
                    reject(error)
                } else {
                    resolve(data)
                }
            })
        })
    }
    // return (arguments.length >= 2)
    //                 ? setDataAsync(arguments[0], arguments[1], arguments[2]) // dataConnector, dataTable
    //                 : crossfilter;
}