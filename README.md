# HeavyAI Crossfilter
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/heavyai/heavyai-crossfilter/blob/master/LICENSE)
[![Security](https://img.shields.io/badge/Security-Report%20a%20Vulnerability-red.svg)](https://github.com/heavyai/heavyai-crossfilter/blob/master/SECURITY.md)
[![GitHub Discussions](https://img.shields.io/badge/GitHub-Discussions-blue?logo=github)](https://github.com/orgs/heavyai/discussions)



JavaScript library for exploring large multivariate datasets in the browser. Based on [CrossFilter](https://github.com/square/crossfilter)

### Table of Contents
- [Quick Start](#quick-start)
- [Synopsis](#synopsis)
- [Testing](#testing)
- [Scripts](#scripts)
- [Contributing](.github/CONTRIBUTING.md)
- [License](LICENSE)

# Quick Start
A full build of `heavyai-crossfilter` is available in the cloned/forked version of this repo. To build your own code changes, however, you can run

```bash
npm install
npm run build
```

# Synopsis

Unlike the original Crossfilter, `heavyai-crossfilter` makes asynchronous network requests to retrieve data. As part of this process, `heavyai-crossfilter` forms SQL queries that are used to retrieved data which will then be rendered by [`heavyai-charting`](https://github.com/heavyai/heavyai-charting).


# Testing

New components in HeavyAI Crossfilter should be unit-tested.  All tests should be in the test directory.

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

# Third-party vendor licenses

A full list of third-party npm packages and their licenses is maintained in [`third_party_licenses/THIRD_PARTY_LICENSES.md`](third_party_licenses/THIRD_PARTY_LICENSES.md). To regenerate it after dependency changes, run:

```sh
npx github:heavyai/js-license-list
```

This requires `node_modules` to be installed (`npm install`). The script is maintained in the [heavyai/js-license-list](https://github.com/heavyai/js-license-list) repo.

Every third-party module from npm that gets includes in the final, distributed bundle has its license verified and license text (if provided) or license type shipped in licenses.txt with the bundle. Licenses must be in the pre-approved list of permissive open-source licenses. If it's necessary to override a license for a module because it's missing or improperly tagged in its package.json, add an entry in license-overrides.json.

License descriptions and public license URLs are maintained in licenses.json as well, but they are not verified and might not be up to date.

## Disclaimer

Variables and function names are used as convention and do not reference any commercial product.


# Security
> [!WARNING]
> **Do not report security vulnerabilities through public GitHub issues!**

NVIDIA takes security seriously. If you discover a vulnerability in heavyai-crossfilter, **DO NOT open a public issue**. Use one of the private reporting channels described in [SECURITY.md](https://github.com/heavyai/heavyai-crossfilter/blob/master/SECURITY.md).


# Support
Join the [HeavyAI GitHub Discussions](https://github.com/orgs/heavyai/discussions) to ask questions, share feedback, and report issues. HeavyAI maintainers review issues, discussions, and pull requests on a best effort basis without guaranteed response timelines.

  
# License
Apache 2.0. See [LICENSE](https://github.com/heavyai/heavyai-crossfilter/blob/master/LICENSE).
