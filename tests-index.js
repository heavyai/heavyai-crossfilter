/**
 * Created by andrelockhart on 5/27/17.
 */
var context = require.context(
    './test', true, /.spec\.js$/
);
context.keys().forEach(context);
