/**
 * Contains Constants for Python Avro
 */

const PRIMITIVE_TYPES = [
  'null',
  'boolean',
  'string',
  'bytes',
  'int',
  'long',
  'float',
  'double'
]

const NAMED_TYPES = [
  'fixed',
  'enum',
  'record',
  'error'
]

const VALID_TYPES = [
  'array',
  'map',
  'union',
  'request',
  'error_union'
].concat(PRIMITIVE_TYPES, NAMED_TYPES)

const SCHEMA_RESERVED_PROPS = [
  'type',
  'name',
  'namespace',
  'fields', // Record
  'items', // Array
  'size', // Fixed
  'symbols', // Enum
  'values', // Map
  'doc'
]

const FIELD_RESERVED_PROPS = [
  'default',
  'name',
  'doc',
  'order',
  'type'
]

const VALID_FIELD_SORT_ORDERS = [
  'ascending',
  'descending',
  'ignore'
]

module.exports = {
  PRIMITIVE_TYPES,
  NAMED_TYPES,
  VALID_TYPES,
  SCHEMA_RESERVED_PROPS,
  FIELD_RESERVED_PROPS,
  VALID_FIELD_SORT_ORDERS
}
