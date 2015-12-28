Mapd-crossfilter
=====

JavaScript library for exploring large multivariate datasets in the browser.

[See official CrossFilter repo](https://github.com/square/crossfilter)

### Installation:

Clone down the repo and run `npm install`.

### Pull Requests:

Attach the appropriate semvar tag below to one of the commit messages in your pull request. This allows Jenkins to publish to npm automatically.

Semvar Tag | Description
--- | ---
`[major]` | major breaking changes
`[minor]` | new features
`[patch]` | Bugfixes, documentation

Jenkins will not let you merge a pull request that contains a missing or multiple semvar tags. **One per Pull Request!**

### Developing mapd-crossfilter and another project at the same time:

**If you have not cloned down the mapd-crossfilter.js repo, do that first.** Then run the following commands:

1. `npm link` - inside the mapd-crossfilter/ repo directory.
2. `npm link @mapd/mapd-crossfilter` - inside your project directory (same level as the `node_modules/` directory).

This overrides the `node_modules` directory and tells your project to use the mapd-crossfilter/ repo instead.

### Updating projects that require mapd-crossfilter after changes are made

Run `npm install @mapd/mapd-crossfilter@latest --save` from within your project to update to the latest version.

