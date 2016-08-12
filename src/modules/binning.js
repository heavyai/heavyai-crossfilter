import moment from "moment";
import {MONTHS, QUARTERS, TIME_LABEL_TO_SECS, TIME_SPANS} from "../constants";

export function formatDateResult(date, label) {
  switch (label) {
    case "second":
    case "minute":
    case "hour":
    case "day":
    case "week":
      return date;
    case "month":
      return MONTHS[moment(date).month()] + " " + moment(date).year();
    case "quarter":
      return QUARTERS[moment(date).quarter() - 1] + " " + moment(date).year();
    case "year":
      return moment(date).year();
    case "decade":
      const yearString = moment(date).year().toString();
      return yearString.slice(0, yearString.length - 1) + "0s";
    case "century":
      return (moment(date).year() + 100).toString().slice(0, 2) + "th Century";
    default:
      return date;
  }
}

export function unBinResults(queryBinParams, results) {
  var numRows = results.length;
  for (var b = 0; b < queryBinParams.length; b++) {
    if (queryBinParams[b] === null) {
      continue;
    }
    var queryBounds = queryBinParams[b].binBounds;
    var numBins = queryBinParams[b].numBins;
    var keyName = "key" + b.toString();
    if (queryBounds[0] instanceof Date && queryBounds[1] instanceof Date) {
      const queryBoundsMsMinMax = [queryBounds[0].getTime(), queryBounds[1].getTime()];
      const label = getTimeBinParams(queryBoundsMsMinMax, numBins);
      const intervalMs = TIME_LABEL_TO_SECS[label] * 1000;
      for (var r = 0; r < numRows; ++r) {
        // jscs:disable
        const result = results[r][keyName];
        const min = result instanceof Date ? result : new Date(queryBoundsMsMinMax[0] + result * intervalMs);

        // jscs:enable
        const max = new Date(min.getTime() + intervalMs);
        results[r][keyName] = [
          {
            value: min,
            alias: formatDateResult(min, label),
          },
          {
            value: max,
            alias: formatDateResult(max, label),
          },
        ];
      }
    } else {
      var unitsPerBin = (queryBounds[1] - queryBounds[0]) / numBins;
      for (var r = 0; r < numRows; ++r) {
        const min = (results[r][keyName] * unitsPerBin) + queryBounds[0];
        const max = min + unitsPerBin;
        results[r][keyName] = [min, max];
      }
    }
  }
  return results;
}

export function getTimeBinParams(timeBounds, maxNumBins) {
  var epochTimeBounds = [(timeBounds[0] * 0.001), (timeBounds[1] * 0.001)];
  var timeRange = epochTimeBounds[1] - epochTimeBounds[0]; // in seconds
  var timeSpans = TIME_SPANS;
  for (var s = 0; s < timeSpans.length; s++) {
    if (timeRange / timeSpans[s].numSeconds < maxNumBins) {
      return timeSpans[s].label;
    }
  }
  return "century"; // default;
}
