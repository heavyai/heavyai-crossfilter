/**
 * Created by andrelockhart on 5/6/17.
 */
import ResultCache from './ResultCache'

/**
 *  Marries connector context with state context. Holds knowledge of one set of tables and all
 *  filtering and organization (group, bin) of it
 */

let instance = null
export default class CrossFilter {
    /******************************************************************
     * properties
     */
    type = 'crossfilter' // todo - as GW said, 'this is some weird shit' (tisws)
    // todo - I'm not a fan of hard coding versions in the source e.g. crossfilter.version = '1.3.11'
    // todo - is the intent to make these private members? if so, use Crockford constructor trick
    _dataTables          = null
    _joinAttrMap         = {}
    _joinStmt            = null
    // FROM clause: assumes all groups and dimensions of this xfilter instance use collection of columns
    // it is most apparent in a multi-layer pointmap (e.g. tweets and contributions ala example)
    _tablesStmt          = null
    _filters             = []
    _targetFilter        = null
    _columnTypeMap       = null
    _compoundColumnMap   = null
    _dataConnector       = null
    _dimensions          = [] // TODO: muy importante - should this be stored here
    _globalFilters       = []
    _cache               = null
    _id                  = 1 // todo - tisws
    // todo - variables declared outside of CrossFilter scope
    cache = null
    /***********   CONSTRUCTOR   ***************/
    constructor() {
        // singleton enforcer
        if(instance) {
            return instance
        }
        else {
            instance = this
        }
    }
    /******************************************************************
     * private methods
     */
    initCrossFilterForAsync(dataConnector, dataTables) {
        this._dataConnector     = dataConnector
        this.cache              = new ResultCache(dataConnector) // this should gc old cache...
        this._dataTables         = dataTables
        this._columnTypeMap      = {}
        this._compoundColumnMap  = {}
        this._joinAttrMap = {}
        this._joinStmt    = null
    }
    /******************************************************************
     * public methods
     */
    setDataAsync(dataConnector, dataTables, joinAttrs) {
        let { _dataTables, _tablesStmt, _joinAttrMap, _joinStmt } = this
        // joinAttrs should be an array of objects with keys
        // table1, table2, attr1, attr2
        this.initCrossFilterForAsync(dataConnector, dataTables)

        if (!Array.isArray(_dataTables)) {
            _dataTables = [_dataTables]
        }
        _tablesStmt = ""
        _dataTables.forEach((table, i) => {
            if (i > 0) {
                _tablesStmt += ","
            }
            _tablesStmt += table
        })
        if (typeof joinAttrs !== 'undefined') {
            _joinStmt = ''
            // todo - tisws: this smells brittle and hard coded (!important for first refactoring)
            joinAttrs.forEach((join, i) => {
                let joinKey = join.table1 < join.table2 ?
                    join.table1 + "." + join.table2 : join.table2 + "." + join.table1
                let tableJoinStmt = join.table1 + "." + join.attr1 + " = "
                    + join.table2 + "." + join.attr2
                if (i > 0) {
                    this._joinStmt += " AND "
                }
                _joinStmt += tableJoinStmt
                _joinAttrMap[joinKey] = tableJoinStmt
            })
        }
        return Promise.all(_dataTables.map(this.getFieldsPromise))
            .then(() => this)
    }
    getFieldsPromise(table) {
        let { _columnTypeMap, _compoundColumnMap } = this
        return new Promise((resolve, reject) => {
            this._dataConnector.getFields(table, (error, columnsArray) => {
                if (error) {
                    reject(error)
                } else {
                    let columnNameCountMap = {}
                    columnsArray.forEach((element) => {
                        let compoundName = table + "." + element.name
                        _columnTypeMap[compoundName] = {
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
                    Reflect.ownKeys(_columnTypeMap).map((key) => {
                        if (columnNameCountMap[_columnTypeMap[key].column] > 1) {
                            _columnTypeMap[key].name_is_ambiguous = true
                        } else {
                            _compoundColumnMap[_columnTypeMap[key].column] = key
                        }
                    })
                    resolve(this)
                }
            })
        })
    }
    // Returns the number of records in this crossfilter, irrespective of any filters.
    size(callback) {
        const { _tablesStmt, _joinStmt, _cache } = this
        if (!callback) {
            console.warn("Warning: Deprecated sync method groupAll.size(). Please use async version");
        }
        let query = "SELECT COUNT(*) as n FROM " + _tablesStmt

        if (_joinStmt !== null) {
            query += " WHERE " + _joinStmt
        }

        const options = {
            eliminateNullRows   : false,
            renderSpec          : null,
            postProcessors      : [(d) => {return d[0].n}]
        }
        if (callback) {
            return _cache.queryAsync(query, options, callback)
        } else {
            return _cache.query(query, options)
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