module.exports = function (wallaby) {
  return {
    files: [
      'mapd-crossfilter.js'
    ],
    tests: [
      'tests.js'
    ],
    testFramework: 'mocha',
    env: {
      type: 'node',
      params: {
        runner: '--harmony --harmony_arrow_functions'
      }
    }
  };
};
