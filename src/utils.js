const isEqual = require('lodash.isequal')

function isString (s) {
  return typeof s === 'string' || s instanceof String
}

function isNumber (n) {
  return typeof n === 'number' || n instanceof Number
}

function isObject (obj) {
  var type = typeof obj
  return (type === 'function') || (type === 'object' && !!obj)
}

module.exports = {
  isString,
  isNumber,
  isObject,
  isEqual
}
