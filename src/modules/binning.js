import { TIME_LABEL_TO_SECS, TIME_SPANS } from "../constants"

export function unBinResults(queryBinParams, results) {
  var numRows = results.length
  for (var b = 0; b < queryBinParams.length; b++) {
    if (queryBinParams[b] === null) continue

    const queryBinParam = queryBinParams[b]
    const { numBins, binBounds, extract } = queryBinParam
    const keyName = "key" + b.toString()

    if (binBounds[0] instanceof Date && binBounds[1] instanceof Date) {
      // jscs:disable
      const binBoundsMsMinMax = [binBounds[0].getTime(), binBounds[1].getTime()]
      const timeBin = queryBinParam.timeBin

      if (extract) {
        for (var r = 0; r < numRows; ++r) {
          const result = results[r][keyName]
          if (result === null) {
            continue
          }
          results[r][keyName] = [
            {
              value: result,
              timeBin,
              isExtract: true,
              extractUnit: timeBin
            }
          ]
        }
        // jscs:enable
      } else {
        const intervalMs = TIME_LABEL_TO_SECS[timeBin] * 1000
        for (var r = 0; r < numRows; ++r) {
          const result = results[r][keyName]
          if (result === null) {
            continue
          }

          // jscs:disable
          const minValue =
            result instanceof Date
              ? result
              : new Date(binBoundsMsMinMax[0] + result * intervalMs)
          const min = {
            value: minValue,
            timeBin,
            isBin: true,
            binUnit: timeBin
          }

          const maxValue = new Date(minValue.getTime() + intervalMs - 1)
          const max = {
            value: maxValue,
            timeBin,
            isBin: true,
            binUnit: timeBin
          }

          // jscs:enable
          results[r][keyName] = [min, max]
        }
      }
    } else {
      var unitsPerBin = (binBounds[1] - binBounds[0]) / numBins
      for (var r = 0; r < numRows; ++r) {
        if (results[r][keyName] === null) {
          continue
        }
        const min = results[r][keyName] * unitsPerBin + binBounds[0]
        const max = min + unitsPerBin
        results[r][keyName] = [min, max]
      }
    }
  }

  return results
}
