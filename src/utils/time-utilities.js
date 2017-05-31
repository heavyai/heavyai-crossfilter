import moment from 'moment'

// convert non-supported time units to moment-compatible inputs
// http://momentjs.com/docs/#/manipulating/
export function  convertTimeToMomentFormat(actualTimeBinUnit) {
    switch (actualTimeBinUnit) {
        case "quarterday":
            actualTimeBinUnit = "hours"
            return 6
        case "decade":
            actualTimeBinUnit = "years"
            return 10
        case "century":
            actualTimeBinUnit = "years"
            return 100
        case "millenium":
            actualTimeBinUnit = "years"
            return 1000
        default:
            return 1
    }
}

export function makeNextTimeInterval(timeInterval, incrementBy, timeUnit) {
    return moment(timeInterval)
        .utc()
        .add(incrementBy, timeUnit)
        .toDate()
}
