# MapD CrossFilter

JavaScript library for exploring large multivariate datasets in the browser. Based on [CrossFilter](https://github.com/square/crossfilter)

### Table of Contents
- [Quick Start](#quick-start)
- [Synopsis](#synopsis)
- [Testing](#testing)
- [Scripts](#scripts)
- [Contributing](.github/CONTRIBUTING.md)
- [License](LICENSE)

# Quick Start
A full build of `mapd-crossfilter` is available in the cloned/forked version of this repo. To build your own code changes, however, you can run

```bash
npm install
npm run build
```

# Synopsis

Unlike the original Crossfilter, `mapd-crossfilter` makes asynchronous network requests to retrieve data. As part of this process, `mapd-crossfilter` forms SQL queries that are used to retrieved data which will then be rendered by [`mapd-charting`](https://github.com/mapd/mapd-charting).


# Testing

New components in MapD-Crossfilter should be unit-tested.  All tests should be in the test directory.

```
+-- src
|   +-- /modules/binning.js
+-- test
|   +-- /binning.unit.spec.js
```

All tests run on
```bash
npm run test
```

To check only unit tests, run:
```bash
npm run test:unit
```

# Scripts

Command | Description
--- | ---
`npm run test` | Runs unit tests and provides coverage info
`npm run test:unit` | Runs unit tests
`npm run build` | Bundles crossfilter
