/**
 * Created by andrelockhart on 5/22/17.
 */
import MapdCon from 'connector'

const USE_MAP = false
const CHART_GROUP = null

export default function BackendScatterExample(dc, crossFilter, colorScheme, dim) {

    const dimension = crossFilter.dimension(null).projectOn(createProjection(dim.x, dim.y, size, color))

    return dc.bubbleRasterChart(node, USE_MAP, CHART_GROUP)
        .crossfilter(crossFilter)
        .con(MapdCon)
        .height(height)
        .width(width)
        .margins({top: 16, right: 24, bottom: 40, left: 48})
        .dimension(dimension)
        .group(dimension)
        .tableName(crossFilter.getTable()[0])
        .cap(cap)
        .othersGrouper(false)
        .xDim(dim.x)
        .yDim(dim.y)
        .renderHorizontalGridLines(true)
        .renderVerticalGridLines(true)
        .xAxisLabel(x.label)
        .yAxisLabel(y.label)
        .sampling(true)
        .enableInteractions(true)
}