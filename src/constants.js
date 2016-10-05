
const SEC = 1
const MIN_IN_SECS = 60
const HOUR_IN_SECS = 3600
const DAY_IN_SECS = 86400
const WEEK_IN_SECS = 604800
const MONTH_IN_SECS = 2592000
const QUARTER_IN_SECS = 10368000
const YEAR_IN_SECS = 31536000
const DECADE_IN_SECS = 315360000

const TIME_LABELS = [
  "second",
  "minute",
  "hour",
  "day",
  "week",
  "month",
  "quarter",
  "year",
  "decade"
]

export const TIME_LABEL_TO_SECS = {
  second: SEC,
  minute: MIN_IN_SECS,
  hour: HOUR_IN_SECS,
  day: DAY_IN_SECS,
  week: WEEK_IN_SECS,
  month: MONTH_IN_SECS,
  quarter: QUARTER_IN_SECS,
  year: YEAR_IN_SECS,
  decade: DECADE_IN_SECS
}

export const TIME_SPANS = TIME_LABELS.map(label => ({
  label,
  numSeconds: TIME_LABEL_TO_SECS[label]
}))

export const DAYS = [
  "Monday",
  "Tuesday",
  "Wednesday",
  "Thursday",
  "Friday",
  "Saturday",
  "Sunday"
]

export const MONTHS = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December"
]

export const QUARTERS = ["Q1", "Q2", "Q3", "Q4"]
