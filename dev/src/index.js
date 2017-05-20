/**
 * Created by andrelockhart on 5/10/17.
 */
import { debounce } from 'lodash'
import d3 from 'd3'
// import thrift from 'thrift/thrift'
// import mapd_types from 'thrift/mapd_types'
// import mapdThrift from 'thrift/mapd.thrift'
import MapdCon from 'connector'
// import * as Crossfilter from '../../src/mapd-crossfilter-api'
import * as Crossfilter from '../../src/JayZ'
import * as dc from 'charting'
//
import BarChartExample from './BarChartExample'
import PieChartExample from './PieChartExample'
import LineChartExample from './LineChartExample'
import RasterChartScatterplotExample from './RasterChartScatterplotExample'

// todo - make example inclusion/render parameterized to facilitate testing

(init())

function init() {
    /*
     * mapdcon is a monad.
     * It provides a MapD specific API to Thrift.  Thrift is used to connect to our
     * database backend.
     */
    /* Before doing anything we must set up a mapd connection, specifying
     * username, password, host, port, and database name */
    MapdCon
        .protocol("http")
        .host("forge.mapd.com")
        .port("9092")
        .dbName("mapd")
        .user("mapd")
        .password("HyperInteractive")
        .connect((error, con) => {
            /*
             *  This instantiates a new crossfilter.
             *  Pass in mapdcon as the first argument to crossfilter, then the
             *  table name, then a label for the data (unused in this example).
             *
             *  to see all available --  con.getTables()
             */
            Crossfilter.crossfilter(con, "flights_donotmodify")
                .then(createCharts)
            /*
             *  Pass instance of crossfilter into our createCharts.
             */
        })
}

function createCharts(crossFilter) {
    let colorScheme = ["#22A7F0", "#3ad6cd", "#d4e666"],
        dim = {
            w: Math.max(document.documentElement.clientWidth, window.innerWidth || 0) - 50,
            h: Math.max(document.documentElement.clientHeight, window.innerHeight || 0) - 200
        }
    /*
     * crossFilter is an object that handles cross-filtered the different
     * dimensions and measures that compose a dashboard's charts.
     * It has a number of methods on it.
     */
    /*
     *  getColumns() will grab all columns from the table along with metadata about
     *  those columns.
     */
    const allColumns = crossFilter.getColumns()
    /*-------------------BASIC COUNT ON CROSSFILTER---------------------------*/
    /*
     *  A basic operation is getting the filtered count and total count
     *  of crossFilter.  This performs that operation.  It is built into DC.
     *  Note that for the count we use crossFilter itself as the dimension.
     */
    const countGroup = crossFilter.groupAll(),
        countWidget  = dc.countWidget(".data-count")
            .dimension(crossFilter) // returns chart, which has group()
            .group(countGroup) // returns chart
    /** shared data **/
    /*
     *  In crossfilter dimensions can function as what we would like to "group by"
     *  in the SQL sense of the term. We'd like to create a bar chart of number of
     *  flights by destination state ("dest_state") - so we create a crossfilter dimension
     *  on "dest_state"
     *
     *  Here lies one of the chief differences between crossfilter.mapd.js and the
     *  original crossfilter.js.  In the original crossfilter you could provide
     *  javascript expressions like d.dest_state.toLowerCase() as part of
     *  dimension, group and order functions.  However since ultimately our
     *  dimensions and measures are transformed into SQL that hit our backend, we
     *  require string expressions. (i.e "extract(year from dep_timestamp))"
     */
    /*
     * To group by a variable, we call group() on the function and then specify
     * a "reducer".  Here we want to get the count for each state, so we use the
     * crossfilter reduceCount() method.
     *
     * More crossfilter Methods here:
     * https://github.com/square/crossfilter/wiki/API-Reference#dimension
     * https://github.com/square/crossfilter/wiki/API-Reference#group-map-reduce
     * https://github.com/square/crossfilter/wiki/API-Reference#group_reduceCount
     */
    /*------------------------  ------------------------------*/
    // BarChartExample(dc, crossFilter, colorScheme, dim)
    // PieChartExample(dc, crossFilter, dim)
    // LineChartExample(d3, dc, crossFilter, colorScheme, dim)
    RasterChartScatterplotExample(d3, dc, Crossfilter)
    /* Calling dc.renderAllAsync() will render all of the charts we set up.  Any
     * filters applied by the user (via clicking the bar chart, scatter plot or
     * dragging the time brush) will automagically call redraw on the charts without
     * any intervention from us
     */
    // dc.renderAllAsync()
    /*--------------------------RESIZE EVENT------------------------------*/

    /* Here we listen to any resizes of the main window.
     On resize we resize the corresponding widgets and call dc.renderAll() to refresh everything
     */
    // window.addEventListener("resize", debounce(reSizeAll, 100))
    // function reSizeAll() {
    //     w = Math.max(document.documentElement.clientWidth, window.innerWidth || 0) - 50
    //     h = Math.max(document.documentElement.clientHeight, window.innerHeight || 0) - 200
    //
    //     // dcBarChart
    //     //     .height(h/1.5)
    //     //     .width(w/2)
    //
    //     dc.redrawAllAsync()
    // }
}