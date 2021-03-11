const MS_IN_SECS = 0.001
const SEC = 1
const MIN_IN_SECS = 60
const HOUR_IN_SECS = 60 * MIN_IN_SECS
const DAY_IN_SECS = 24 * HOUR_IN_SECS
const WEEK_IN_SECS = 7 * DAY_IN_SECS
const MONTH_IN_SECS = 30 * DAY_IN_SECS
const QUARTER_IN_SECS = 3 * MONTH_IN_SECS
const YEAR_IN_SECS = 365 * DAY_IN_SECS
const DECADE_IN_SECS = 10 * YEAR_IN_SECS

export const TIME_LABEL_TO_SECS = {
  millisecond: MS_IN_SECS,
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

export const DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

export const MONTHS = [
  "Jan",
  "Feb",
  "Mar",
  "Apr",
  "May",
  "Jun",
  "Jul",
  "Aug",
  "Sep",
  "Oct",
  "Nov",
  "Dec"
]

export const QUARTERS = ["Q1", "Q2", "Q3", "Q4"]
