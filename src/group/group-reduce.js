export default function reduce(expressions) {
    // _reduceTableSet = {};
    let  reduceSubExpressions = expressions,
    reduceExpression = "",
    reduceVars = ""

    if (!arguments.length) {
        return reduceSubExpressions
    }

    const numExpressions = expressions.length;
    for (var e = 0; e < numExpressions; e++) {
        if (e > 0) {
            reduceExpression += ",";
            reduceVars += ",";
        }
        if (e === targetSlot
            && targetFilter !== null
            && targetFilter !== dimensionIndex
            && filters[targetFilter] !== "") {

            // this is the old way
            // reduceExpression += "AVG(CAST(" + filters[targetFilter] + " AS INT))"
            reduceExpression += " AVG(CASE WHEN " + filters[targetFilter] + " THEN 1 ELSE 0 END)";
        } else {

            /*
             * if (expressions[e].expression in columnTypeMap) {
             *   _reduceTableSet[columnTypeMap[expressions[e].expression].table] =
             *     (_reduceTableSet[columnTypeMap[expressions[e].expression].table] || 0) + 1;
             *  }
             */

            var agg_mode = expressions[e].agg_mode.toUpperCase();

            if (agg_mode === "CUSTOM") {
                reduceExpression += expressions[e].expression;
            } else if (agg_mode === "COUNT") {
                if (expressions[e].filter) {
                    reduceExpression += "COUNT(CASE WHEN " + expressions[e].filter + " THEN 1 END)";
                } else {
                    if (typeof expressions[e].expression !== "undefined") {
                        reduceExpression += "COUNT(" + expressions[e].expression + ")";
                    } else {
                        reduceExpression += "COUNT(*)";
                    }
                }
            } else { // should check for either sum, avg, min, max
                if (expressions[e].filter) {
                    reduceExpression += agg_mode + "(CASE WHEN " + expressions[e].filter +
                        " THEN " +  expressions[e].expression + " END)";
                } else {
                    reduceExpression += agg_mode + "(" + expressions[e].expression + ")";
                }
            }
        }
        reduceExpression += " AS " + expressions[e].name;
        reduceVars += expressions[e].name;
    }
    return group
}
