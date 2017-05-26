/**
 * Created by andrelockhart on 5/24/17.
 */
import { debounce } from 'lodash'
import rangey from 'lodash/range'

function getDomainBounds (column, groupAll, callback) {
    groupAll.reduce([
        {expression: column, agg_mode: "min", name: "minimum"},
        {expression: column, agg_mode: "max", name: "maximum"}
    ]).valuesAsync(true)
        .then(callback)
}

function domainFromBoundsAndRange (min, max, range) {
    return rangey(0, range.length).map((_, i) => min + Math.round(i * max / (range.length - 1)))
}

function width () {
    return document.documentElement.clientWidth - 30
}

function height () {
    return (Math.max(document.documentElement.clientHeight, window.innerHeight || 0) - 200)
}

function resizeChart (dc, chart, heightDivisor) {
    if(typeof chart.map === "function"){
        chart.map().resize()
        chart.isNodeAnimate = false
    }
    chart
        .width(width())
        .height(height()/heightDivisor)
        .renderAsync()
    dc.redrawAllAsync()
}

export default function PolyMapWithBackendRenderingExample(d3, dc, Crossfilter) {

    const config = {
        table: "contributions_donotmodify",
        valueColumn: "contributions_donotmodify.amount",
        joinColumn: "contributions_donotmodify.contributor_zipcode",
        polyTable: "zipcodes",
        polyJoinColumn: "ZCTA5CE10",
        timeColumn: "contrib_date",
        timeLabel: "Number of Contributions",
        domainBoundMin: 0,
        domainBoundMax: 2600,
        numTimeBins: 423
    }

    new MapdCon()
        .protocol("https")
        .host("metis.mapd.com")
        .port("443")
        .dbName("mapd")
        .user("mapd")
        .password("HyperInteractive")
        .connect(function(error, con) {
            Crossfilter.crossfilter(con, ["contributions_donotmodify", "zipcodes"], [{
                table1: "contributions_donotmodify",
                attr1: "contributor_zipcode",
                table2: "zipcodes",
                attr2: "ZCTA5CE10"
            }])
                .then((cf) => {
                    Crossfilter.crossfilter(con, "contributions_donotmodify")
                        .then(cf2 => {
                            createPolyMap(cf, con, dc, config, cf2)
                            createTimeChart(cf, dc, config, cf2)
                        })
                })
        })

    function createPolyMap(crossFilter, con, dc, config, cf2) {

        const parent = document.getElementById("poly-map-with-backend-rendering-example"),
            dim = crossFilter.dimension("zipcodes.rowid"), // Values to join on.
            grp = dim.group().reduceAvg("contributions_donotmodify.amount", "avgContrib") // Values to color on.

        getDomainBounds(config.valueColumn, cf2.groupAll(), (domainBounds) => {
            // Can set colorDomain directly or use domainFromBoundsAndRange to generate a .
            const colorRange = ["#115f9a","#1984c5","#22a7f0","#48b5c4","#76c68f","#a6d75b","#c9e52f","#d0ee11","#d0f400"],
             colorDomain = domainFromBoundsAndRange(config.domainBoundMin, config.domainBoundMax, colorRange)

            const polyMap = dc.rasterChart(parent, true)
                .con(con)
                .height(height()/1.5)
                .width(width())
                .mapUpdateInterval(750) // ms
                .mapStyle("mapbox://styles/mapbox/light-v8")

            const polyLayer = dc.rasterLayer("polys")
                .dimension(dim)
                .group(grp)
                .fillColorAttr('avgContrib')
                .defaultFillColor("green")
                .fillColorScale(d3.scale.linear().domain(colorDomain).range(colorRange))

            polyMap.pushLayer("polys", polyLayer).init()
                .then(() => {
                // polyMap.borderWidth(zoomToBorderWidth(polyMap.map().getZoom()))
                // Keeps the border widths reasonable regardless of zoom level.
                polyMap.map().on("zoom", () => {
                    // polyMap.borderWidth(zoomToBorderWidth(polyMap.map().getZoom()))
                })

                dc.renderAllAsync()

                window.addEventListener("resize", debounce(function(){ resizeChart(polyMap, 1.5) }, 500))
            })
        })
    }

    function createTimeChart(crossFilter, dc, config, cf2) {

        const parentTimechart = document.getElementById("poly-map-with-backend-rendering-timechart")

        getDomainBounds(config.timeColumn, cf2.groupAll(), (timeChartBounds) => {
            const timeChartDimension = crossFilter.dimension(config.timeColumn),
             timeChartGroup = timeChartDimension
                .group()
                .reduceCount("*")

            // debugger
            const timeChart = dc.lineChart(parentTimechart)
                .width(width())
                .height(height()/2.5)
                .elasticY(true)
                .renderHorizontalGridLines(true)
                .brushOn(true)
                .xAxisLabel("Time")
                .yAxisLabel(config.timeLabel)
                .dimension(timeChartDimension)
                .group(timeChartGroup)
                .binParams({
                    numBins: config.numTimeBins,
                    binBounds: [timeChartBounds.minimum, timeChartBounds.maximum]
                })

            timeChart.x(d3.time.scale.utc().domain([timeChartBounds.minimum, timeChartBounds.maximum]))
            timeChart.yAxis().ticks(5)
            timeChart
                .xAxis()
                .scale(timeChart.x())
                .tickFormat(dc.utils.customTimeFormat)
                .orient('bottom')

            dc.renderAllAsync()

            window.addEventListener("resize", debounce(function () { resizeChart(timeChart, 2.5) }, 500))
        })
    }
}