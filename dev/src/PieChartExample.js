/**
 * Created by andrelockhart on 5/17/17.
 */

export const PIE_EXTERNAL_RADIUS_PADDING = 32
export const PIE_INNER_RADIUS_MULTIPLIER = 0.2

export default function PieChartExample(dc, crossFilter, dim) {

    const pieChartDimension = crossFilter.dimension("dest_state")
    const pieChartGroup = pieChartDimension.group().reduceCount()

    const innerRadius = Math.min(dim.w, dim.h) * PIE_INNER_RADIUS_MULTIPLIER

    return dc.pieChart('.pie-chart-example')
        .height(dim.h/1.5)
        .width(dim.w/2)
        .innerRadius(innerRadius)
        .cap(12)
        .othersGrouper(false)
        .externalRadiusPadding(PIE_EXTERNAL_RADIUS_PADDING)
        .pieStyle("donut")
        .ordering('asc')
        .measureLabelsOn(true)
        .dimension(pieChartDimension)
        .group(pieChartGroup)
}