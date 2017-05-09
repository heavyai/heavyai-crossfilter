/**
 * Created by andrelockhart on 5/9/17.
 */
/** third party libs **/
import moment from 'moment'
import { pull } from 'lodash'
/** crossfilter functions **/
import { convertTimeToMomentFormat, makeNextTimeInterval } from '../utils/time-utilities'

function binParams(binParamsIn) {
    if (!arguments.length) {
        return this._binParams
    }
    this._binParams = binParamsIn
    return this
}

function fillUnidimensionalTimeBin(group, results) {
    let actualTimeBinUnit   = group._binParams()[0].timeBin,
        incrementBy         = convertTimeToMomentFormat(actualTimeBinUnit),
        lastResult          = null,
        valueKeys           = [],
        filledResults   = []

    results.forEach((result) => {
        if (lastResult) {
            let lastTime         = lastResult.key0,
                currentTime      = moment(result.key0).utc().toDate(),
                nextTimeInterval = makeNextTimeInterval(lastTime, incrementBy, actualTimeBinUnit),
                interval         = Math.abs(nextTimeInterval - lastTime)

            // todo - see if filtering array is simpler or lodash function
            while (nextTimeInterval < currentTime) {
                let timeDiff = currentTime - nextTimeInterval

                if (timeDiff > interval / 2) { // we have a missing time value
                    let insertResult = { key0: nextTimeInterval }

                    for (let k = 0; k < valueKeys.length; k++) {
                        insertResult[valueKeys[k]] = 0
                    }
                    filledResults.push(insertResult)
                }
                nextTimeInterval = makeNextTimeInterval(nextTimeInterval, incrementBy, actualTimeBinUnit)
            }
        } else { // first result - get its keys
            valueKeys = pull(Object.keys(result), 'key0')
        }
        filledResults.push(result)
        lastResult = result
    })
    return filledResults
}
// todo - simplify!
function fillNonTemporalBins(results, queryBinParams, numDimensions, numResults) {
    let allDimsBinned   = true, // we don't handle for now mixed cases
        totalArraySize  = 1,
        dimensionSizes  = [],
        dimensionSums   = [],
        filledResults   = []

    // todo - tisws: this appears to yield inconsistent results, e.g. some bin params
    // todo - processed, others not, depending upon the position of null bin param in array
    for (let b = 0; b < queryBinParams.length; b++) {
        if (queryBinParams[b] === null) {
            allDimsBinned = false
            break
        }
        totalArraySize *= queryBinParams[b].numBins
        dimensionSizes.push(queryBinParams[b].numBins)
        dimensionSums.push(b === 0 ? 1 : (queryBinParams[b].numBins * dimensionSums[b - 1]))
    }
    dimensionSums.reverse()
    if (allDimsBinned) {
        numDimensions = dimensionSizes.length

        // make an array filled with 0 of length numDimensions
        let counters    = Array.from(new Array(5)).forEach(null), // todo - I think this is the same as old code below?
            // let counters = Array.apply(0, Array(numDimensions))
            //     .map(Number.prototype.valueOf, 0)
            allKeys     = Object.keys(results[0]),
            valueKeys   = pull(allKeys, !'key')

        for (let i = 0; i < totalArraySize; i++) {
            let result = {}
            for (let k = 0; k < valueKeys.length; k++) {
                result[valueKeys[k]] = 0; // Math.floor(Math.random() * 100);
            }
            for (let d = 0; d < numDimensions; d++) { // now add dimension keys
                result["key" + d] = counters[d];
            }
            filledResults.push(result);
            for (let d = numDimensions - 1; d >= 0; d--) { // now add dimension keys
                counters[d] += 1;
                if (counters[d] < dimensionSizes[d]) {
                    break;
                } else {
                    counters[d] = 0;
                }
            }
        }
        for (let r = 0; r < numResults; r++) {
            let index = 0;
            for (let d = 0; d < numDimensions; d++) {
                index += (results[r]["key" + d] * dimensionSums[d])
            }
            filledResults[index] = results[r]
        }
        return (filledResults)
    }
}

export default function fillBins(group, queryBinParams, results = []) {
    if (!group._fillMissingBins) return results
    let numDimensions   = queryBinParams.length,
        numResults      = results.length,
        numTimeDims     = 0

    queryBinParams.forEach((dim) => {
        if (dim.timeBin) {
            numTimeDims++
        }
    })
    // we only support filling bins when there is one time dimension
    // and it is the only dimension
    if (numDimensions === 1 && numTimeDims === 1) {
        return fillUnidimensionalTimeBin(group, results)
    }
    else if (numTimeDims === 0 && numResults > 0) {
        return fillNonTemporalBins(results, queryBinParams, numDimensions, numResults)
    }
    else {
        return results
    }
}