var webpack = require("webpack");
var path = require("path");

module.exports = {
  context: __dirname,
  entry: {
    "mapd-crossfilter": "./src/mapd-crossfilter.js"
  },
  output: {
    path: __dirname + "/dist",
    filename: "[name].js",
    libraryTarget: "umd",
    library: "crossfilter"
  },
  devtool: "source-map",
  module: {
    loaders: [
      {
        test: /\.js?$/,
        exclude: /node_modules/,
        include: path.resolve(__dirname, 'src'),
        loader: "babel"
      }
    ]
  },
  plugins: [
    new webpack.DefinePlugin({
      "process.env": {
        NODE_ENV: JSON.stringify("production")
      }
    }),
    new webpack.optimize.OccurenceOrderPlugin(),
    new webpack.optimize.DedupePlugin(),
  ],
  resolve: {
    extensions: ["", ".js"]
  }
};
