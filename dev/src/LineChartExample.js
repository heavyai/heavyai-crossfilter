const reduceMultiExpression2 = [
    {
        expression  : "dep_timestamp",
        agg_mode    :"min",
        name        : "minimum"
    },
    {
        expression  : "dep_timestamp",
        agg_mode    :"max",
        name        : "maximum"
    }
]

export default function LineChartExample(d3, dc, crossFilter, colorScheme, dim) {

    /* We would like to bin or histogram the time values.  We do this by
     * invoking setBinParams on the group.  Here we are asking for 400 equal
     * sized bins from the min to the max of the time range
     */
    /*  We create the time chart as a line chart
     *  with the following parameters:
     *
     *  Width and height - as above
     *
     *  elasticY(true) - cause the y-axis to scale as filters are changed
     *
     *  renderHorizontalGridLines(true) - add grid lines to the chart
     *
     *  brushOn(true) - Request a filter brush to be added to the chart - this
     *  will allow users to drag a filter window along the time chart and filter
     *  the rest of the data accordingly
     *
     */
    const timeChartDimension = crossFilter.dimension("dep_timestamp"),
        timeChartGroup       = timeChartDimension.group()
                                                 .reduceCount()

    // debugger
    crossFilter
        .groupAll()
        .reduceMulti(reduceMultiExpression2)
        .valuesAsync(true)
        .then((timeChartBounds) => {

        let dcTimeChart = dc.lineChart('.line-chart-example')
            .width(dim.w)
            .height(dim.h/2.5)
            .elasticY(true)
            .renderHorizontalGridLines(true)
            .brushOn(true)
            .xAxisLabel('Departure Time')
            .yAxisLabel('# Flights')
            .dimension(timeChartDimension)
            .group(timeChartGroup)
            .binParams({
                numBins: 400,
                binBounds: [timeChartBounds.minimum, timeChartBounds.maximum]
            })
            // necessary for range chart: todo - wait until after refactor
            // .rangeChartEnabled(true)
            // dcTimeChart._rangeMeasure

        /* Set the x and y axis formatting with standard d3 functions */
        dcTimeChart
            .x(d3.time.scale.utc().domain([timeChartBounds.minimum, timeChartBounds.maximum]))
            .yAxis().ticks(5)

        dcTimeChart
            .xAxis()
            .scale(dcTimeChart.x())
            .tickFormat(dc.utils.customTimeFormat)
            .orient('top')

        console.log('I am rendering now')
        dc.renderAllAsync()
    })
}
