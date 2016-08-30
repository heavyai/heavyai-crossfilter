Mapd-crossfilter
=====

JavaScript library for exploring large multivariate datasets in the browser.

[See official CrossFilter repo](https://github.com/square/crossfilter)

## Overview

Crossfilter is a multi-dimensionsal filtering library. However, unlike the original, `mapd-crossfilter` makes asynchronous network requests to retrieve the data. As part of this process, `mapd-crossfilter` forms SQL queries that are used to retrieved the data to be rendered by DC. 

## Development Guidelines

### Use Asynchronous Methods

Asynchronous Thrift client methods must always be used. Synchronous methods are deprecated and cause a bad user experience.

To ensure that the asynchronous version of the method is called, simply pass in a callback.

```js
// Bad
try {
  const response = client.query(query, options)
} catch (e) {
  throw e
}

// Good
client.query(query, options, (error, response) => {
  if (error) {
    callback(error)
  }  else {
    callback(null, response)
  }
})
```

You can even go one step further and wrap this in a Promise.

```js
// better
new Promise ((resolve, reject) => {
  client.query(query, options, (error, response) => {
    if (error) {
      reject(error)
    }  else {
      resolve(response)
    }
  })
})
```

### Prefer Functions Over Methods 

To avoid overloading the crossfilter classes (`dimensions`, `group`, `groupAll`, etc), do not add methods to crossfilter when it does not need state. If it is pure, then abstract it outside the crossfilter scope as a function instead.

```js

// bad

dimension = {
  helper (a) {
    return a + 1
  },
  method (a) {
    return this.helper(a)
  }
}

// good

function helper () {
  return a + 1
}

dimension = {
  method (a) {
    return helper(a)
  }
}
```

### Testing

Any addition to `madp-crossfilter` must be unit tested. All tests are located in `/test/`.

### Linting 

All code in `mapd-crossfilter/src` must be linted. The linting guidelines can be found in `.jcsrc`. 

## npm Scripts

Command | Description
--- | ---
`npm run test` | Runs unit tests and provides coverage info
`npm run build` | Bundles crossfilter for examples
`npm run lint` | Lints src files
