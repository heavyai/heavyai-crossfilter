Mapd-crossfilter
=====

JavaScript library for exploring large multivariate datasets in the browser.

Based on [CrossFilter](https://github.com/square/crossfilter)

## Overview

Unlike the original Crossfilter, `mapd-crossfilter` makes asynchronous network requests to retrieve the data. As part of this process, `mapd-crossfilter` forms SQL queries that are used to retrieved the data to be rendered by [`mapd-charting`](https://github.com/mapd/mapd-charting).

## npm Scripts

Command | Description
--- | ---
`npm run test` | Runs unit tests and provides coverage info
`npm run test:unit` | Runs unit tests
`npm run build` | Bundles crossfilter
