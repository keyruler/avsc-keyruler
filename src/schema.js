const constants = require('./constants')
const utils = require('./utils')

/**
 * Constructs the Schema from the JSON text.
 */
function parse (jsonString) {
  // Parse the JSON
  let jsonData
  if (jsonString instanceof Object) {
    jsonData = jsonString
  } else {
    try {
      jsonData = JSON.parse(jsonString)
    } catch (e) {
      throw new Error(`Error parsing JSON: ${jsonString}, error = ${e}`)
    }
  }

  const names = new Names()

  return makeAvscObject(jsonData, names)
}

/**
 * Build Avro Schema from data parsed out of JSON string.
 *
 * @param {Object} names A Name object (tracks seen names and default space)
 */
function makeAvscObject (jsonData, names, options) {
  if (names === undefined) {
    names = {}
  }

  if (jsonData instanceof Array) {
    // JSON array (union)
    return new UnionSchema(jsonData, names)
  } else if (jsonData instanceof Object) {
    // JSON object (non-union)
    const type = jsonData.type
    const otherProps = getOtherProps(jsonData, constants.SCHEMA_RESERVED_PROPS)
    const logicalType = jsonData.logicalType
    if (constants.PRIMITIVE_TYPES.includes(type)) {
      return new PrimitiveSchema(type, otherProps, logicalType)
    } else if (constants.NAMED_TYPES.includes(type)) {
      const name = jsonData.name
      const namespace = jsonData.namespace === undefined ? jsonData.namespace : names.defaultNamespace
      if (type === 'fixed') {
        const size = jsonData.size
        return new FixedSchema(name, namespace, size, names, otherProps, logicalType)
      } else if (type === 'enum') {
        const symbols = jsonData.symbols
        const doc = jsonData.doc
        return new EnumSchema(name, namespace, symbols, names, doc, otherProps, logicalType)
      } else if (['record', 'error'].includes(type)) {
        const fields = jsonData.fields
        const doc = jsonData.doc
        return new RecordSchema(name, namespace, fields, names, type, doc, otherProps, logicalType)
      } else {
        throw new Error(`Unknown Named Type: ${type}`)
      }
    } else if (constants.VALID_TYPES.includes(type)) {
      if (type === 'array') {
        const items = jsonData.items
        return new ArraySchema(items, names, otherProps, logicalType)
      } else if (type === 'map') {
        const values = jsonData.values
        return new MapSchema(values, names, otherProps, logicalType)
      } else if (type === 'error_union') {
        const declaredErrors = jsonData.declared_errors
        return new ErrorUnionSchema(declaredErrors, names, logicalType)
      } else {
        throw new Error(`Unknown Valid Type: ${type}`)
      }
    } else if (type === undefined) {
      throw new Error(`No "type" property: ${jsonData}`)
    } else {
      throw new Error(`Undefined type: ${type}`)
    }
  } else if (constants.PRIMITIVE_TYPES.includes(jsonData)) {
    // JSON string (primitive)
    return new PrimitiveSchema(jsonData)
  } else {
    // Not for us!
    throw new Error(`Could not make an Avro Schema object from ${jsonData}.`)
  }
}

/**
 * Retrieve the non-reserved properties from a dictionary of properties
 *
 * @param {object} allProps The props to filter
 * @param {array} reservedProps The set of reserved properties to exclude
 */
function getOtherProps (allProps, reservedProps) {
  const props = {}
  for (const [k, v] of Object.entries(allProps)) {
    if (!reservedProps.includes(k)) {
      props[k] = v
    }
  }

  return props
}

/**
 * Class to describe Avro name.
 */
class Name {
  /**
   * Formulate full name according to the specification.
   *
   * @param {} nameAttr: name value read in schema or None.
   * @param {} spaceAttr: namespace value read in schema or None.
   * @param {} defaultSpace: the current default space or None.
   */
  constructor (nameAttr, spaceAttr, defaultSpace) {
    // Ensure valid ctor args
    if (!(utils.isString(nameAttr) || nameAttr === undefined)) {
      throw new Error('Name must be non-empty string or None.')
    } else if (nameAttr === '') {
      throw new Error('Name must be non-empty string or None.')
    }

    if (!(utils.isString(spaceAttr) || spaceAttr === undefined)) {
      throw new Error('Space must be non-empty string or None.')
    } else if (spaceAttr === '') {
      throw new Error('Space must be non-empty string or None.')
    }

    if (!(utils.isString(defaultSpace) || defaultSpace === undefined)) {
      throw new Error('Default must be non-empty string or None.')
    } else if (defaultSpace === '') {
      throw new Error('Default must be non-empty string or None.')
    }

    this._full = undefined

    if (nameAttr.indexOf('.') < 0) {
      if (spaceAttr !== undefined && spaceAttr !== '') {
        this._full = `${spaceAttr}.${nameAttr}`
      } else {
        if (defaultSpace !== undefined && defaultSpace !== '') {
          this._full = `${defaultSpace}.${nameAttr}`
        } else {
          this._full = nameAttr
        }
      }
    } else {
      this._full = nameAttr
    }
  }

  equals (other) {
    if (!(other instanceof Name)) {
      return false
    }

    return (this.fullname === other.fullname)
  }

  get fullname () {
    return this._full
  }

  /**
   * Back out a namespace from full name.
   */
  getSpace () {
    if (this._full === undefined) {
      return undefined
    }

    if (this._full.indexOf('.') > 0) {
      // TODO: Check that this is correct
      return this._full.split('.', 1).reverse()[0]
    } else {
      // TODO: This might not be correct.
      return undefined
    }
  }
}

/**
 * Track name set and default namespace during parsing.
 */
class Names {
  constructor (defaultNamespace) {
    this.names = {}
    this.defaultNamespace = defaultNamespace
  }

  hasName (nameAttr, spaceAttr) {
    const test = new Name(nameAttr, spaceAttr, this.defaultNamespace).fullname
    return Object.prototype.hasOwnProperty.call(this.names, test)
  }

  getName (nameAttr, spaceAttr) {
    const test = new Name(nameAttr, spaceAttr, this.defaultNamespace).fullname
    if (!Object.prototype.hasOwnProperty.call(this.names, test)) {
      return undefined
    }
    return this.names[test]
  }

  /**
   * Given a properties, return properties with namespace removed if
   * it matches the own default namespace
   * @param {object} properties
   */
  pruneNamespace (properties) {
    if (this.defaultNamespace === undefined) {
      // I have no default -- no change
      return properties
    }
    if (!Object.keys(properties).includes('namespace')) {
      // he has no namespace - no change
      return properties
    }
    if (properties.namespace !== this.defaultNamespace) {
      // we're different - leave his stuff alone
      return properties
    }

    // we each have a namespace and it's redundant. delete his.
    const prunable = Object.assign({}, properties)
    delete prunable.namespace
    return prunable
  }

  /**
   * Add a new schema object to the name set.
   *
   * @param {string} nameAttr name value read in schema
   * @param {string} spaceAttr namespace value read in schema.
   * @param {*} newSchema
   *
   * @return: the Name that was just added.
   */
  addName (nameAttr, spaceAttr, newSchema) {
    const toAdd = new Name(nameAttr, spaceAttr, this.defaultNamespace)

    if (toAdd.fullname in constants.VALID_TYPES) {
      throw new Error(`${toAdd.fullname} is a reserved type name.`)
    } else if (Object.prototype.hasOwnProperty.call(this.names, toAdd.fullname)) {
      throw new Error(`The name "${toAdd.fullname}" is already in use.`)
    }

    this.names[toAdd.fullname] = newSchema
    return toAdd
  }
}

/**
 * Base class for all Schema classes.
 */
class Schema {
  constructor (type, otherProps, logicalType) {
    // Ensure valid ctor args
    if (!utils.isString(type)) {
      throw new Error('Schema type must be a string.')
    } else if (!constants.VALID_TYPES.includes(type)) {
      throw new Error(`${type} is not a valid type.`)
    }

    if (logicalType !== undefined) {
      this.logicalType = logicalType
    }

    // add members
    if (this._props === undefined) {
      this._props = {}
    }
    this.setProp('type', type)
    this.type = type
    this._props = { ...this._props, ...(otherProps || {}) }
  }

  get props () {
    return this._props
  }

  /**
   * Dictionary of non-reserved properties
   */
  get otherProps () {
    return getOtherProps(this._props, constants.SCHEMA_RESERVED_PROPS)
  }

  // utility functions to manipulate properties dict
  getProp (key) {
    return this._props[key]
  }

  setProp (key, value) {
    this._props[key] = value
  }

  toString () {
    return JSON.stringify(this.toJson())
  }

  /**
   * Converts the schema object into its AVRO specification representation.
   * Schema types that have names (records, enums, and fixed) must
   * be aware of not re-defining schemas that are already listed
   * in the parameter names.
   * @param {object} names
   */
  toJson (names) {
    throw new Error('Must be implemented by subclasses.')
  }
}

/**
 * Named Schemas specified in NAMED_TYPES.
 */
class NamedSchema extends Schema {
  constructor (type, name, namespace, names, otherProps, logicalType) {
    // Ensure valid ctor args
    if (name === undefined) {
      throw new Error('Named Schemas must have a non-empty name.')
    } else if (!utils.isString(name)) {
      throw new Error('The name property must be a string.')
    } else if (namespace !== undefined && !utils.isString(namespace)) {
      throw new Error('The namespace property must be a string.')
    }

    // Call parent ctor
    super(type, otherProps, logicalType)

    // Add class members
    const newName = names.addName(name, namespace, this) // TODO

    // Store name and namespace as they were read in origin schema
    this.setProp('name', name)
    if (namespace !== undefined) {
      this.setProp('namespace', newName.get_space())
    }

    // Store full name as calculated from name, namespace
    this._fullname = newName.fullname
  }

  nameRef (names) {
    if (this.namespace === names.default_namespace) {
      return this.name
    } else {
      return this.fullname
    }
  }

  // read-only properties
  get name () {
    return this.getProp('name')
  }

  get namespace () {
    return this.getProp('namespace')
  }

  get fullname () {
    return this._fullname
  }
}

class Field {
  constructor (type, name, hasDefault, defaultValue, order, names, doc, otherProps) {
    // Ensure valid ctor args
    if (name === undefined) {
      throw new Error('Fields must have a non-empty name.')
    } else if (!utils.isString(name)) {
      throw new Error('The name property must be a string.')
    } else if (order !== undefined && !constants.VALID_FIELD_SORT_ORDERS.includes(order)) {
      throw new Error(`The order property ${order} is not valid.`)
    }

    // add members
    this._props = {}
    this._hasDefault = hasDefault
    this._props = { ...this._props, ...(otherProps || {}) }

    let typeSchema
    if (utils.isString(type) && names !== undefined && names.hasName(type, undefined)) {
      typeSchema = names.getName(type, undefined)
    } else {
      try {
        typeSchema = makeAvscObject(type, names)
      } catch (e) {
        throw new Error(`Type property "${type}" not a valid Avro schema: ${e}`)
      }
    }
    this.setProp('type', typeSchema)
    this.setProp('name', name)
    this.type = typeSchema
    this.name = name

    // TODO(hammer): check to ensure default is valid
    if (hasDefault) {
      this.setProp('default', defaultValue)
    }
    if (order !== undefined) {
      this.setProp('order', order)
    }
    if (doc !== undefined) {
      this.setProp('doc', doc)
    }
  }

  // read-only properties
  get default () {
    return this.getProp('default')
  }

  get hasDefault () {
    return this._hasDefault
  }

  get order () {
    return this.getProp('order')
  }

  get doc () {
    return this.getProp('doc')
  }

  get props () {
    return this._props
  }

  // Read-only property dict. Non-reserved properties
  /**
   * Dictionary of non-reserved properties
   */
  get otherProps () {
    return getOtherProps(this._props, constants.FIELD_RESERVED_PROPS)
  }

  // utility functions to manipulate properties dict
  getProp (key) {
    return this._props[key]
  }

  setProp (key, value) {
    this._props[key] = value
  }

  toString () {
    return JSON.stringify(this.toJson())
  }

  toJson (names) {
    if (names === undefined) {
      names = new Names()
    }

    const toDump = Object.assign({}, this.props)
    toDump.type = this.type.toJson(names)
    return toDump
  }

  equals (that) {
    const toCmp = JSON.parse(this)
    return utils.isEqual(toCmp, JSON.parse(that))
  }
}

/**
 * Valid primitive types are in PRIMITIVE_TYPES.
 */
class PrimitiveSchema extends Schema {
  constructor (type, otherProps, logicalType) {
    // Ensure valid ctor args
    if (!constants.PRIMITIVE_TYPES.includes(type)) {
      throw new Error(`${type} is not a valid primitive type.`)
    }

    // Call parent ctor
    super(type, otherProps, logicalType)
    this.fullname = type
  }

  toJson (names) {
    if (Object.keys(this.props).length === 1) {
      return this.fullname
    } else {
      return this.props
    }
  }

  equals (that) {
    return utils.isEqual(this.props, that.props)
  }
}

class FixedSchema extends NamedSchema {
  constructor (name, namespace, size, names, otherProps, logicalType) {
    if (!utils.isNumber(size) || size < 0) {
      throw new Error('Fixed Schema requires a valid positive integer for size property.')
    }

    // Call parent ctor
    super('fixed', name, namespace, names, otherProps, logicalType)

    // Add class members
    this.setProp('size', size)
  }

  // read-only properties
  get size () {
    return this.getProp('size')
  }

  toJson (names) {
    if (names === undefined) {
      names = new Names()
    }

    if (Object.keys(names.names).includes(this.fullname)) {
      return this.nameRef(names)
    } else {
      names.names[this.fullname] = this
      return names.pruneNamespace(this.props)
    }
  }

  equals (that) {
    return utils.isEqual(this.props, that.props)
  }
}

class EnumSchema extends NamedSchema {
  constructor (name, namespace, symbols, names, doc, otherProps, logicalType) {
    // Ensure valid ctor args
    if (!Array.isArray(symbols)) {
      throw new Error('Enum Schema requires a JSON array for the symbols property.')
    } else if (symbols.map(utils.isString).includes(false)) {
      throw new Error('Enum Schema requires all symbols to be JSON strings.')
    } else if (new Set(symbols).size < symbols.length) {
      throw new Error(`Duplicate symbol: ${symbols}`)
    }

    // Call parent ctor
    super('enum', name, namespace, names, otherProps, logicalType)

    // Add class members
    this.setProp('symbols', symbols)
    if (doc !== undefined) {
      this.setProp('doc', doc)
    }
  }

  // read-only properties
  get symbols () {
    return this.getProp('symbols')
  }

  get doc () {
    return this.getProp('doc')
  }

  toJson (names) {
    if (names === undefined) {
      names = new Names()
    }

    if (this.fullname in names.names) {
      return this.nameRef(names)
    } else {
      names.names[this.fullname] = this
      return names.pruneNamespace(this.props)
    }
  }

  equals (that) {
    return utils.isEqual(this.props, that.props)
  }
}

class RecordSchema extends NamedSchema {
  /**
   * We're going to need to make message parameters too.
   * @param {*} fieldData
   * @param {*} names
   */
  static makeFieldObjects (fieldData, names) {
    const fieldObjects = []
    const fieldNames = []
    for (const field of fieldData) {
      if (field instanceof Object) {
        const type = field.type
        const name = field.name

        // null values can have a default value of None
        let hasDefault = false
        let defaultValue
        if (Object.prototype.hasOwnProperty.call(field, 'default')) {
          hasDefault = true
          defaultValue = field.default
        }

        const order = field.order
        const doc = field.doc
        const otherProps = getOtherProps(field, constants.FIELD_RESERVED_PROPS)
        const newField = new Field(type, name, hasDefault, defaultValue, order, names, doc, otherProps)
        // make sure field name has not been used yet
        if (fieldNames.includes(newField.name)) {
          throw new Error(`Field name ${newField.name} already in use.`)
        }
        fieldNames.push(newField.name)
        fieldObjects.push(newField)
      } else {
        throw new Error(`Not a valid field: ${field}`)
      }
    }

    return fieldObjects
  }

  constructor (name, namespace, fields, names, schemaType = 'record', doc, otherProps, logicalType) {
    // Ensure valid ctor args
    if (fields === undefined) {
      throw new Error('Record schema requires a non-empty fields property.')
    } else if (!Array.isArray(fields)) {
      throw new Error('Fields property must be a list of Avro schemas.')
    }

    // Call parent ctor (adds own name to namespace, too)
    if (schemaType === 'request') {
      throw new Error('Schema type request in not implemented')
    } else {
      super(schemaType, name, namespace, names, otherProps, logicalType)
    }

    let oldDefault
    if (schemaType === 'record') {
      oldDefault = names.defaultNamespace
      names.defaultNamespace = new Name(name, namespace, names.defaultNamespace).getSpace()
    }

    // Add class members
    const fieldObjects = RecordSchema.makeFieldObjects(fields, names)
    this.setProp('fields', fieldObjects)
    if (doc !== undefined) {
      this.setProp('doc', doc)
    }

    if (schemaType === 'record') {
      names.defaultNamespace = oldDefault
    }
  }

  // read-only properties
  get fields () {
    return this.getProp('fields')
  }

  get doc () {
    return this.getProp('doc')
  }

  get fieldsDict () {
    const fieldsDict = {}
    for (const field of this.fields) {
      fieldsDict[field.name] = field
    }
    return fieldsDict
  }

  toJson (names) {
    if (names === undefined) {
      names = new Names()
    }

    // Request records don't have names
    if (this.type === 'request') {
      return this.fields.map(f => f.toJson(names))
    }

    if (Object.keys(names.names).includes(this.fullname)) {
      return this.nameRef(names)
    } else {
      names.names[this.fullname] = this
    }

    const toDump = names.pruneNamespace(Object.assign({}, this.props))
    toDump.fields = this.fields.map(f => f.toJson(names))
    return toDump
  }

  equals (that) {
    const toCmp = JSON.parse(this)
    return utils.isEqual(toCmp, JSON.parse(that))
  }
}

/**
 * Names is a dictionary of schema objects
 */
class UnionSchema extends Schema {
  constructor (schemas, names, logicalType) {
    // Ensure valid ctor args
    if (!Array.isArray(schemas)) {
      throw new Error('Union schema requires a list of schemas.')
    }

    // Call parent ctor
    super('union', logicalType)

    // Add class members
    const schemaObjects = []
    for (const schema of schemas) {
      let newSchema
      if (utils.isString(schema) && names.hasName(schema, undefined)) {
        newSchema = names.getName(schema, undefined)
      } else {
        try {
          newSchema = makeAvscObject(schema, names)
        } catch (e) {
          throw new Error(`Union item must be a valid Avro schema: ${e}`)
        }
      }

      // check the new schema
      if (constants.VALID_TYPES.includes(newSchema.type) &&
          !constants.NAMED_TYPES.includes(newSchema.type) &&
          schemaObjects.map(s => s.type).includes(newSchema.type)
      ) {
        throw new Error(`${newSchema.type} type already in Union`)
      } else if (newSchema.type === 'union') {
        throw new Error('Unions cannot contain other unions.')
      } else {
        schemaObjects.push(newSchema)
      }
    }
    this._schemas = schemaObjects
  }

  // read-only properties
  get schemas () {
    return this._schemas
  }

  toJson (names) {
    if (names === undefined) {
      names = new Names()
    }

    const toDump = []
    for (const schema of this.schemas) {
      toDump.push(schema.toJson(names))
    }
    return toDump
  }

  equals (that) {
    const toCmp = JSON.parse(this)
    return utils.isEqual(toCmp, JSON.parse(that))
  }
}

class ArraySchema extends Schema {
  constructor (items, names, otherProps, logicalType) {
    // Call parent ctor
    super('array', otherProps, logicalType)
    // Add class members

    let itemsSchema
    if (utils.isString(items) && names.hasName(items, undefined)) {
      itemsSchema = names.getName(items, undefined)
    } else {
      try {
        itemsSchema = makeAvscObject(items, names)
      } catch (e) {
        throw new Error(`Items schema (${items}) not a valid Avro schema: ${e} (known names: ${Object.keys(names.names)})`)
      }
    }
    this.setProp('items', itemsSchema)
  }

  // read-only properties
  get items () {
    return this.getProp('items')
  }

  toJson (names) {
    if (names === undefined) {
      names = new Names()
    }

    const toDump = Object.assign({}, this.props)
    const itemsSchema = this.getProp('items')
    toDump.items = itemsSchema.toJson(names)
    return toDump
  }

  equals (that) {
    const toCmp = JSON.parse(this)
    return utils.isEqual(toCmp, JSON.parse(that))
  }
}

class MapSchema extends Schema {
  constructor (values, names, otherProps, logicalType) {
    // Call parent ctor
    super('map', otherProps, logicalType)

    // Add class members
    let valuesSchema
    if (utils.isString(values) && names.hasName(values, undefined)) {
      valuesSchema = names.getName(values, undefined)
    } else {
      try {
        valuesSchema = makeAvscObject(values, names)
      } catch (e) {
        throw new Error('Values schema not a valid Avro schema.')
      }
    }

    this.setProp('values', valuesSchema)
  }

  // read-only properties
  get values () {
    return this.getProp('values')
  }

  toJson (names) {
    if (names === undefined) {
      names = new Names()
    }

    const toDump = Object.assign({}, this.props)
    toDump.values = this.getProp('values').toJson(names)
    return toDump
  }

  equals (that) {
    const toCmp = JSON.parse(this)
    return utils.isEqual(toCmp, JSON.parse(that))
  }
}

class ErrorUnionSchema extends UnionSchema {
  constructor (schemas, names, logicalType) {
    // Prepend "string" to handle system errors
    super(['string'].concat(schemas), names, logicalType)
  }

  toJson (names) {
    if (names === undefined) {
      names = new Names()
    }
    const toDump = []
    for (const schema of this.schemas) {
      // Don't print the system error schema
      if (schema.type === 'string') {
        continue
      }

      toDump.push(schema.toJson(names))
    }
    return toDump
  }
}

module.exports = {
  parse,
  Schema,
  Name
}
