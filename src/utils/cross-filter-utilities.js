/**
 * Created by andrelockhart on 5/6/17.
 */
import moment from 'moment'

const relativeDateRegex = /DATE_ADD\(([^,|.]+), (DATEDIFF\(\w+, ?\d+, ?\w+\(\)\)[-+0-9]*|[-0-9]+), ([0-9]+|NOW\(\))\)/g

function formatFilterValue(value, wrapInQuotes, isExact) {
    const valueType = type(value)
    if (valueType === 'string') {

        let escapedValue = value.replace(/'/g, "''")
        if (!isExact) {
            escapedValue = escapedValue.replace(/%/g, "\\%")
            escapedValue = escapedValue.replace(/_/g, "\\_")
        }
        return wrapInQuotes ? "'" + escapedValue + "'" : escapedValue
    } else if (valueType === 'date') {
        return "TIMESTAMP(0) '" + value.toISOString().slice(0, 19).replace("T", " ") + "'"
    } else {
        return value
    }
}

export function isRelative(sqlStr) {
    return /DATE_ADD\(([^,|.]+), (DATEDIFF\(\w+, ?\d+, ?\w+\(\)\)[-+0-9]*|[-0-9]+), ([0-9]+|NOW\(\))\)|NOW\(\)/g.test(sqlStr)
}

export function replaceRelative(sqlStr) {
        const withRelative = sqlStr.replace(relativeDateRegex, (match, datepart, number) => {
        if (isNaN(number)) {
            const num = Number(number.slice(number.lastIndexOf(")")+1))
            if (isNaN(num)) {
                return formatFilterValue(moment().utc().startOf(datepart).toDate(), true)
            } else {
                return formatFilterValue(moment().add(num, datepart).utc().startOf(datepart).toDate(), true)
            }
        } else {
            return formatFilterValue(moment().add(number, datepart).toDate(), true)
        }
    })
    return withRelative.replace(/NOW\(\)/g, formatFilterValue(moment().toDate(), true))
}

