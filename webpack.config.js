const webpack = require("webpack");
const path = require("path");

module.exports = {
  context: __dirname,
  entry: {
    "mapd-crossfilter": "./src/CrossfilterWrapper.js"
  },
  output: {
    path: __dirname + "/dist",
    filename: "[name].js",
    libraryTarget: "umd",
    library: "crossfilter"
  },
  module: {
    loaders: [
      {
        test: /\.js?$/,
        exclude: /node_modules/,
        loader: "babel-loader"
      }
    ]
  },
  plugins: [
    new webpack.DefinePlugin({
      "process.env": {
        NODE_ENV: JSON.stringify("production")
      }
    })
  ],
  resolve: {
    extensions: [".js"]
  }
};
