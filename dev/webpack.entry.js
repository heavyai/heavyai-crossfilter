/**
 * Created by andrelockhart on 5/12/17.
 */
module.exports = {
    app: [
        "babel-polyfill",
        "./hackoid",
        "script-loader!../../connector/build/thrift/browser/thrift",
        "script-loader!../../connector/build/thrift/browser/mapd.thrift",
        "script-loader!../../connector/build/thrift/browser/mapd_types",
        "../../connector/src/mapd-con-es6.js",
        // "./sass/Styles.index",
        "./src/index"
    ]
}
