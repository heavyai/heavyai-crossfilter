/*------------------------CHART 1 EXAMPLE------------------------------*/
/*
 *  We create a horizontal bar chart with the data specified above (count by destination
 *  state) by using a dc.rowChart (i.e. a horizontal bar chart)
 *
 *  We invoke the following options on the rowChart using chaining.
 *
 *  Height and width - match the containing div
 *
 *  elasticX - a dc option to cause the axis to rescale as other filters are
 *  applied
 *
 *  cap(20) - Only show the top 20 groups.  By default crossFilter will sort
 *  the dimension expression (here, "dest_state"), by the reduce expression (here, count),
 *  so we end up with the top 20 destination states ordered by count.
 *
 *  othersGrouper(false) - We only would like the top 20 states and do not want
 *  a separate bar combining all other states.
 *
 *  ordinalColors(colorScheme) - we want to color the bars by dimension, i.e. dest_state,
 *  using the color ramp defined above (an array of rgb or hex values)
 *
 *  measureLabelsOn(true) - a mapd.dc.js add-on which allows not only the dimension
 *  labels (i.e. Texas) to be displayed but also the measures (i.e. the number
 *  of flights with Texas as dest_state)
 *
 *  Simple Bar Chart Example using DC api here:
 *  https://github.com/dc-js/dc.js/blob/master/web/docs/api-latest.md
 */
export default function BarChartExample(dc, crossFilter, colorScheme, dim) {

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
    const rowChartDimension = crossFilter.dimension("dest_state")
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
    const rowChartGroup = rowChartDimension.group().reduceCount()

    return dc.rowChart('.bar-chart-example')
        .height(dim.h/1.5)
        .width(dim.w/2)
        .elasticX(true)
        .cap(20)
        .othersGrouper(false)
        .ordinalColors(colorScheme)
        .measureLabelsOn(true)
        .dimension(rowChartDimension)
        .group(rowChartGroup)
        .autoScroll(true)
}
