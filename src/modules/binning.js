import moment from "moment";
import {DAYS, MONTHS, QUARTERS, TIME_LABEL_TO_SECS, TIME_SPANS} from "../constants";

export function formatDateResult(date, label) {
  switch (label) {
    case "second":
    case "minute":
    case "hour":
      return date;
    case "day":
    case "week":
      return moment(date).format("MMMM Do YYYY");
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

export function formatExtractResult(number, label) {
  switch (label) {
    case "isodow":
      return DAYS[number];
    case "month":
      return MONTHS[number];
    case "quarter":
      return QUARTERS[number];
    default:
      return number;
  }
}

export function unBinResults(queryBinParams, results) {
  var numRows = results.length;
  for (var b = 0; b < queryBinParams.length; b++) {
    if (queryBinParams[b] === null) continue;

    const queryBinParam = queryBinParams[b];
    const { numBins, binBounds, extract } = queryBinParam;
    const keyName = "key" + b.toString();

    if (binBounds[0] instanceof Date && binBounds[1] instanceof Date) {
      const binBoundsMsMinMax = [binBounds[0].getTime(), binBounds[1].getTime()];
      const timeBin = queryBinParam.timeBin === "auto" || !queryBinParam.timeBin ? autoBinParams(binBoundsMsMinMax, numBins) : queryBinParam.timeBin;

      if (extract) {
        for (var r = 0; r < numRows; ++r) {
          const result = results[r][keyName];
          results[r][keyName] = [{
            value: result,
            alias: formatExtractResult(result - 1, timeBin),
          },];
        }

      } else {
        const intervalMs = TIME_LABEL_TO_SECS[timeBin] * 1000;
        for (var r = 0; r < numRows; ++r) {
          const result = results[r][keyName];

          // jscs:disable
          const minValue = result instanceof Date ? result : new Date(binBoundsMsMinMax[0] + result * intervalMs)
          const min = {
            value: minValue,
            alias: formatDateResult(minValue, timeBin),
            timeBin,
          };

          const maxValue = new Date(minValue.getTime() + intervalMs)
          const max = {
            value: maxValue,
            alias: formatDateResult(maxValue, timeBin),
            timeBin,

          };

          // jscs:enable
          results[r][keyName] = [min, max];
        }
      }
    } else {
      var unitsPerBin = (binBounds[1] - binBounds[0]) / numBins;
      for (var r = 0; r < numRows; ++r) {
        const min = (results[r][keyName] * unitsPerBin) + binBounds[0];
        const max = min + unitsPerBin;
        results[r][keyName] = [min, max];
      }
    }
  }

  return results;
}

export function autoBinParams(timeBounds, maxNumBins) {
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
