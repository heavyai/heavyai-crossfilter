const path = require('path');
const webpack = require('webpack');
const HtmlWebpackPlugin = require('html-webpack-plugin');
const ExtractTextPlugin = require('extract-text-webpack-plugin');

const webpackEntry = require('./webpack.entry');
const srcDir = path.resolve(__dirname);
const publicDir = path.resolve(__dirname, 'public');
const distDir = path.resolve(__dirname, '..', 'dist')

module.exports = {
    context: srcDir,
    devtool: 'source-map',
    resolve: {
        extensions: ['.js', '.json'],
        alias: {
            connector   : path.resolve(__dirname, '../../connector/src/mapd-con-es6'),
            // thrift : path.resolve(__dirname, './connector/thrift/browser/'),
            // dimension : path.resolve(__dirname, '../src/dimension/'),
            // group      : path.resolve(__dirname, '../src/group/'),
            charting    : path.resolve(__dirname, '../../charting/'),
            mapbox      : path.resolve(__dirname, './node_modules/mapbox-gl/dist/mapbox-gl.js')
        }
    },
    entry: webpackEntry,
    output: {
        filename: 'mapd-crossfilter.js',
        path: distDir,
        publicPath: '/',
        sourceMapFilename: 'main.map'
    },
    plugins: [
        new webpack.NamedModulesPlugin(),
        new HtmlWebpackPlugin({
            template: path.join(publicDir, 'index.html'),
            // where to find the html template
            path: publicDir,
            // where to put the generated file
            filename: 'index.html'
            // the output file name
        })
        // new webpack.optimize.UglifyJsPlugin({
        //     // Eliminate comments
        //     comments: false,
        //     // Compression specific options
        //     compress: {
        //         // remove warnings
        //         warnings: false,
        //         // Drop console statements
        //         drop_console: false
        //     }
        // })
    ],
    devServer: {
        contentBase: srcDir,
        // match the output path
        publicPath: '/',
        // match the output `publicPath`
        historyApiFallback: true,
        port: 3000
    },
    node: {
        fs: 'empty'
    },
    // externals: {
    //     "mapbox-gl": 'mapboxgl'
    // },
    module: {
        noParse: /(mapbox-gl)\.js$/,
        rules: [
            {
                test: /\.js$/,
                exclude: /node_modules/,
                use: {
                    loader: 'babel-loader'
                }
            },
            {
                test: /\.scss$/,
                exclude: /node_modules/,
                loaders: ['style-loader', 'css-loader', 'sass-loader']
            },
            {
                test: /\.css$/,
                exclude: /node_modules/,
                use: [
                    'style-loader',
                    'css-loader'
                ]
            },
            {
                test: /\.(jpg|jpeg|png|gif|ico|svg)$/,
                loader: 'url-loader',
                query: {
                    limit: 10000, // use data url for assets <= 10KB
                    name: 'assets/[name].[hash].[ext]'
                }
            }
        ]
    }
};
