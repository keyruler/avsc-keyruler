const utils = require('./utils')
const schema = require('./schema')
const constants = require('../src/constants')

// eslint-disable-next-line no-unused-vars
const Tap = require('./tap')
// eslint-disable-next-line no-unused-vars
const Schema = schema.Schema

const INT_MIN_VALUE = -((1 << 31) >>> 0)
const INT_MAX_VALUE = ((1 << 31) >>> 0) - 1
// Can't do this bitwise
const LONG_MIN_VALUE = -9223372036854775808
const LONG_MAX_VALUE = 9223372036854775807

const _valid = {
  null: (s, d) => d === null,
  boolean: (s, d) => typeof d === 'boolean',
  string: (s, d) => utils.isString(d),
  bytes: (s, d) => Buffer.isBuffer(d),
  int: (s, d) => utils.isNumber(d) && (INT_MIN_VALUE <= d && d <= INT_MAX_VALUE),
  long: (s, d) => (utils.isNumber(d) && (LONG_MIN_VALUE <= d && d <= LONG_MAX_VALUE)),
  float: (s, d) => utils.isNumber(d),
  double: (s, d) => utils.isNumber(d),
  fixed: (s, d) => Buffer.isBuffer(d) && d.length === s.size,
  enum: (s, d) => s.symbols.includes(d),
  array: (s, d, o) => Array.isArray(d) && d.map(item => validate(s.items, item, o)).every(item => item),
  map: (s, d, o) => (
    utils.isObject(d) &&
    Object.keys(d).map(item => utils.isString(item)).every(item => item) &&
    Object.values(d).map(value => validate(s.values, value, o)).every(value => value)
  ),
  union: (s, d, o) => s.schemas.map(branch => validate(branch, d, o)).some(v => v),
  record: (s, d, o) => {
    const fieldNames = s.fields.map(f => f.name)
    return utils.isObject(d) && s.fields.map(f => validate(f.type, d[f.name], o)).every(f => f) && Object.keys(d).every(k => fieldNames.includes(k))
  }
}
_valid['error_union'] = _valid['union']
_valid['error'] = _valid['request'] = _valid['record']

/**
 * Determines if a python datum is an instance of a schema.
 *
 * @param {} expectedSchema Schema to validate against.
 * @param {} datum: Datum to validate.
 * @returns True if the datum is an instance of the schema.
 */
function validate (expectedSchema, datum, options) {
  const logicalType = expectedSchema.logicalType
  try {
    if (logicalType !== undefined && options !== undefined && options.logicalTypes !== undefined && utils.isObject(options.logicalTypes[logicalType])) {
      const logicalTypeObject = options.logicalTypes[logicalType]
      return logicalTypeObject.validateBeforeToValue(datum, expectedSchema, options)
    } else {
      return _valid[expectedSchema.type](expectedSchema, datum, options)
    }
  } catch (e) {
    throw new Error(`Unknown Avro schema type: ${expectedSchema.type}`)
  }
}

function createSchemaResolutionError (failMessage, writersSchema, readerSchema) {
  if (writersSchema !== undefined) {
    failMessage += `\nWriter's Schema: ${JSON.stringify(writersSchema, undefined, 2)}`
  }
  if (readerSchema !== undefined) {
    failMessage += `\nReader's Schema: ${JSON.stringify(readerSchema, undefined, 2)}`
  }

  return new Error(failMessage)
}

function createAvroTypeError (expectedSchema, datum) {
  return new Error(`The datum ${JSON.stringify(datum)} is not an example of the schema ${JSON.stringify(expectedSchema, undefined, 2)}`)
}

/**
 * Deserialize Avro-encoded data into a Python data structure.
 */
class DatumReader {
  static checkProps (schemaOne, schemaTwo, propList) {
    for (const prop of propList) {
      if (schemaOne[prop] !== schemaTwo[prop]) {
        return false
      }
    }
    return true
  }

  static matchSchemas (writersSchema, readersSchema) {
    const wType = writersSchema.type
    const rType = readersSchema.type
    if ([wType, rType].includes('union') || [wType, rType].includes('error_union')) {
      return true // Can we do more checks around this to fail earlier?
    } else if (constants.PRIMITIVE_TYPES.includes(wType) && constants.PRIMITIVE_TYPES.includes(rType) && wType === rType) {
      return true
    } else if (wType === 'record' && rType === 'record' && DatumReader.checkProps(writersSchema, readersSchema, ['fullname'])) {
      return true
    } else if (wType === 'error' && rType === 'error' && DatumReader.checkProps(writersSchema, readersSchema, ['fullname'])) {
      return true
    } else if (wType === 'request' && rType === 'request') {
      return true
    } else if (wType === 'fixed' && rType === 'fixed' && DatumReader.checkProps(writersSchema, readersSchema, ['fullname', 'size'])) {
      return true
    } else if (wType === 'enum' && rType === 'enum' && DatumReader.checkProps(writersSchema, readersSchema, ['fullname'])) {
      return true
    } else if (wType === 'map' && rType === 'map' && DatumReader.checkProps(writersSchema.values, readersSchema.values, ['type'])) {
      return true
    } else if (wType === 'array' && rType === 'array' && DatumReader.checkProps(writersSchema.items, readersSchema.items, ['type'])) {
      return true
    }

    // Handle schema promotion
    if (wType === 'int' && ['long', 'float', 'double'].includes(rType)) {
      return true
    } else if (wType === 'long' && ['float', 'double'].includes(rType)) {
      return true
    } else if (wType === 'float' && rType === 'double') {
      return true
    }
    return false
  }

  /**
   * As defined in the Avro specification, we call the schema encoded
   * in the data the "writer's schema", and the schema expected by the
   * reader the "reader's schema".
   */
  constructor (writersSchema, readersSchema, options) {
    this.writersSchema = writersSchema
    this.readersSchema = readersSchema
    this.options = options || {}
  }

  /**
   *
   * @param {Tap} tap
   *
   * @returns {Promise} Promise resolving with the read data.
   */
  read (tap) {
    if (this.readersSchema === undefined) {
      this.readersSchema = this.writersSchema
    }

    return this.readData(this.writersSchema, this.readersSchema, tap)
  }

  /**
   *
   * @param {Schema} writersSchema
   * @param {Schema} readersSchema
   * @param {Tap} tap
   *
   * @returns {Promise} Promise resolving with the read data.
   */
  async readData (writersSchema, readersSchema, tap) {
    // schema matching
    if (!DatumReader.matchSchemas(writersSchema, readersSchema)) {
      return Promise.reject(createSchemaResolutionError('Schemas do not match.', writersSchema, readersSchema))
    }

    // schema resolution: reader's schema is a union, writer's schema is not
    if (!['union', 'error_union'].includes(writersSchema.type) && ['union', 'error_union'].includes(readersSchema.type)) {
      for (const s of readersSchema.schemas) {
        if (DatumReader.matchSchemas(writersSchema, s)) {
          return this.readData(writersSchema, s, tap)
        }
      }
      return Promise.reject(createSchemaResolutionError(`Schemas do not match.`, writersSchema, readersSchema))
    }

    let datum
    // function dispatch for reading data based on type of writer's schema
    switch (writersSchema.type) {
      case 'null':
        datum = null
        break
      case 'boolean':
        datum = await tap.readBoolean()
        break
      case 'string':
        datum = await tap.readString()
        break
      case 'int':
        datum = await tap.readInt()
        break
      case 'long':
        datum = await tap.readLong()
        break
      case 'float':
        datum = await tap.readFloat()
        break
      case 'double':
        datum = await tap.readDouble()
        break
      case 'bytes':
        datum = await tap.readBytes()
        break
      case 'fixed':
        datum = await tap.readFixed(writersSchema.size)
        break
      case 'enum':
        datum = await this.readEnum(writersSchema, readersSchema, tap)
        break
      case 'array':
        datum = await this.readArray(writersSchema, readersSchema, tap)
        break
      case 'map':
        datum = await this.readMap(writersSchema, readersSchema, tap)
        break
      case 'union':
      case 'error_union':
        datum = await this.readUnion(writersSchema, readersSchema, tap)
        break
      case 'record':
      case 'error':
      case 'request':
        datum = await this.readRecord(writersSchema, readersSchema, tap)
        break
      default:
        throw new Error(`Cannot read unknown schema type: ${writersSchema.type}`)
    }

    const logicalType = readersSchema.logicalType
    if (logicalType !== undefined && this.options.logicalTypes !== undefined && utils.isObject(this.options.logicalTypes[logicalType])) {
      const logicalTypeObject = this.options.logicalTypes[logicalType]
      if (logicalTypeObject.validateBeforeFromValue(datum, readersSchema, this.options)) {
        return logicalTypeObject.fromValue(datum, readersSchema)
      } else {
        // TODO: Logging this some way would be nice, the spec says to ignore it.
      }
    }

    return datum
  }

  /**
   *
   * @param {*} writersSchema
   * @param {Tap} tap
   */
  skipData (writersSchema, tap) {
    switch (writersSchema.type) {
      case 'null': return
      case 'boolean':
        return tap.skipBoolean()
      case 'string':
        return tap.skipUtf8()
      case 'int':
        return tap.skipInt()
      case 'long':
        return tap.skipLong()
      case 'float':
        return tap.skipFloat()
      case 'double':
        return tap.skipDouble()
      case 'bytes':
        return tap.skipBytes()
      case 'fixed':
        return tap.skipFixed(writersSchema.size)
      case 'enum':
        return this.skipEnum(writersSchema, tap)
      case 'array':
        return this.skipArray(writersSchema, tap)
      case 'map':
        return this.skipMap(writersSchema, tap)
      case 'union':
      case 'error_union':
        return this.skipUnion(writersSchema, tap)
      case 'record':
      case 'error':
      case 'request':
        return this.skipRecord(writersSchema, tap)
      default:
        throw new Error(`Unknown schema type: ${writersSchema.type}`)
    }
  }

  /**
   * An enum is encoded by a int, representing the zero-based position
   * of the symbol in the schema.
   * @param {Schema} writersSchema
   * @param {Schema} readersSchema
   * @param {Tap} tap
   */
  async readEnum (writersSchema, readersSchema, tap) {
    // read data
    const indexOfSymbol = tap.readInt()
    if (indexOfSymbol >= writersSchema.symbols.length) {
      throw createSchemaResolutionError(
        `Can't access enum index ${indexOfSymbol} for enum with ${writersSchema.symbols.length} symbols`,
        writersSchema,
        readersSchema
      )
    }
    const readSymbol = writersSchema.symbols[indexOfSymbol]

    // schema resolution
    if (!readersSchema.symbols.includes(readSymbol)) {
      throw createSchemaResolutionError(`Symbol ${readSymbol} not present in Reader's Schema`, writersSchema, readersSchema)
    }

    return readSymbol
  }

  /**
   * @param {Schema} writersSchema
   * @param {Tap} tap
   */
  skipEnum (writersSchema, tap) {
    return tap.skipInt()
  }

  /**
   * Arrays are encoded as a series of blocks.
   *
   * Each block consists of a long count value,
   * followed by that many array items.
   * A block with count zero indicates the end of the array.
   * Each item is encoded per the array's item schema.
   *
   * If a block's count is negative,
   * then the count is followed immediately by a long block size,
   * indicating the number of bytes in the block.
   * The actual count in this case
   * is the absolute value of the count written.
   * @param {Schema} writersSchema
   * @param {Schema} readersSchema
   * @param {Tap} tap
   */
  async readArray (writersSchema, readersSchema, tap) {
    const readItems = []
    let blockCount = tap.readLong()
    while (blockCount !== 0) {
      if (blockCount < 0) {
        blockCount = -blockCount
        // Block size
        tap.readLong()
      }
      for (let i = 0; i < blockCount; i++) {
        readItems.push(await this.readData(writersSchema.items, readersSchema.items, tap))
      }
      blockCount = tap.readLong()
    }
    return readItems
  }

  /**
   * @param {Schema} writersSchema
   * @param {Tap} tap
   */
  skipArray (writersSchema, tap) {
    let blockCount = tap.readLong()
    while (blockCount !== 0) {
      if (blockCount < 0) {
        const blockSize = tap.readLong()
        tap.skip(blockSize)
      } else {
        for (let i = 0; i < blockCount; i++) {
          this.skipData(writersSchema.items, tap)
        }
      }
      blockCount = tap.readLong()
    }
  }

  /**
   * Maps are encoded as a series of blocks.
   *
   * Each block consists of a long count value,
   * followed by that many key/value pairs.
   * A block with count zero indicates the end of the map.
   * Each item is encoded per the map's value schema.
   *
   * If a block's count is negative,
   * then the count is followed immediately by a long block size,
   * indicating the number of bytes in the block.
   * The actual count in this case
   * is the absolute value of the count written.
   * @param {Schema} writersSchema
   * @param {Schema} readersSchema
   * @param {Tap} tap
   */
  async readMap (writersSchema, readersSchema, tap) {
    const readItems = {}
    let blockCount = tap.readLong()
    while (blockCount !== 0) {
      if (blockCount < 0) {
        blockCount = -blockCount
        blockCount = tap.readLong()
      }
      for (let i = 0; i < blockCount; i++) {
        const key = tap.readString()
        readItems[key] = await this.readData(writersSchema.values, readersSchema.values, tap)
      }
      blockCount = tap.readLong()
    }
    return readItems
  }

  /**
   * @param {Schema} writersSchema
   * @param {Tap} tap
   */
  skipMap (writersSchema, tap) {
    let blockCount = tap.readLong()
    while (blockCount !== 0) {
      if (blockCount < 0) {
        const blockSize = tap.readLong()
        tap.skip(blockSize)
      } else {
        for (let i = 0; i < blockCount; i++) {
          tap.skipString()
          this.skipData(writersSchema.values, tap)
        }
      }
      blockCount = tap.readLong()
    }
  }

  /**
   * A union is encoded by first writing a long value indicating
   * the zero-based position within the union of the schema of its value.
   * The value is then encoded per the indicated schema within the union.
   * @param {Schema} writersSchema
   * @param {Schema} readersSchema
   * @param {Tap} tap
   */
  async readUnion (writersSchema, readersSchema, tap) {
    // schema resolution
    const indexOfSchema = tap.readLong()
    if (indexOfSchema >= writersSchema.schemas.length) {
      throw createSchemaResolutionError(
        `Can't access branch index ${indexOfSchema} for union with ${writersSchema.schemas.length} branches`,
        writersSchema,
        readersSchema
      )
    }
    const selectedWritersSchema = writersSchema.schemas[indexOfSchema]

    // read data
    return this.readData(selectedWritersSchema, readersSchema, tap)
  }

  /**
   * @param {Schema} writersSchema
   * @param {Schema} readersSchema
   * @param {Tap} tap
   */
  skipUnion (writersSchema, tap) {
    const indexOfSchema = tap.readLong()
    if (indexOfSchema >= writersSchema.schemas.length) {
      // fail_msg = "Can't access branch index %d for union with %d branches"\
      //           % (indexOfSchema, len(writersSchema.schemas))
      // raise SchemaResolutionException(fail_msg, writersSchema)
      throw createSchemaResolutionError(
        `Can't access branch index ${indexOfSchema} for union with ${writersSchema.schemas.length} branches`,
        writersSchema
      )
    }
    return this.skipData(writersSchema.schemas[indexOfSchema], tap)
  }

  /**
   * A record is encoded by encoding the values of its fields
   * in the order that they are declared. In other words, a record
   * is encoded as just the concatenation of the encodings of its fields.
   * Field values are encoded per their schema.
   *
   * Schema Resolution:
   *  * the ordering of fields may be different: fields are matched by name.
   *  * schemas for fields with the same name in both records are resolved
   *    recursively.
   *  * if the writer's record contains a field with a name not present in the
   *    reader's record, the writer's value for that field is ignored.
   *  * if the reader's record schema has a field that contains a default value,
   *    and writer's schema does not have a field with the same name, then the
   *    reader should use the default value from its field.
   *  * if the reader's record schema has a field with no default value, and
   *    writer's schema does not have a field with the same name, then the
   *    field's value is unset.
   *
   * @param {Schema} writersSchema
   * @param {Schema} readersSchema
   * @param {Tap} tap
   */
  async readRecord (writersSchema, readersSchema, tap) {
    // schema resolution
    const readersFieldsDict = readersSchema.fieldsDict
    const readRecord = {}
    for (const field of writersSchema.fields) {
      const readersField = readersFieldsDict[field.name]
      if (readersField !== undefined) {
        const fieldVal = await this.readData(field.type, readersField.type, tap)
        readRecord[field.name] = fieldVal
      } else {
        this.skipData(field.type, tap)
      }
    }

    // fill in default values
    const dictEntries = Object.entries(readersFieldsDict)
    if (dictEntries.length > Object.keys(readRecord).length) {
      const writersFieldsDict = writersSchema.fieldsDict
      for (const [fieldName, field] of dictEntries) {
        if (!Object.prototype.hasOwnProperty.call(writersFieldsDict, fieldName)) {
          if (field.hasDefault) {
            const fieldVal = await this.readDefaultValue(field.type, field.default)
            readRecord[field.name] = fieldVal
          } else {
            throw createSchemaResolutionError(`No default value for field ${fieldName}`, writersSchema, readersSchema)
          }
        }
      }
    }

    return readRecord
  }

  skipRecord (writersSchema, tap) {
    for (const field of writersSchema.fields) {
      this.skipData(field.type, tap)
    }
  }

  /**
   * Basically a JSON Decoder?
   * @param {Schema} field_schema
   * @param {*} defaultValue
   */
  async readDefaultValue (fieldSchema, defaultValue) {
    switch (fieldSchema.type) {
      case 'null':
        return null
      case 'boolean':
      case 'int':
      case 'long':
      case 'float':
      case 'double':
      case 'enum':
      case 'string':
        return defaultValue
      case 'fixed':
      case 'bytes':
        return Buffer.from(defaultValue)
      case 'array':
        const readArray = []
        for (const jsonVal of defaultValue) {
          const itemVal = await this.readDefaultValue(fieldSchema.items, jsonVal)
          readArray.push(itemVal)
        }
        return readArray
      case 'map':
        const readMap = {}
        for (const [key, jsonVal] of Object.entries(defaultValue)) {
          const mapVal = await this.readDefaultValue(fieldSchema.values, jsonVal)
          readMap[key] = mapVal
        }
        return readMap
      case 'union':
      case 'error_union':
        return this.readDefaultValue(fieldSchema.schemas[0], defaultValue)
      case 'record':
        const readRecord = {}
        for (const field of fieldSchema.fields) {
          let jsonVal = defaultValue[field.name]
          if (jsonVal === undefined) {
            jsonVal = field.default
          }
          const fieldVal = await this.readDefaultValue(field.type, jsonVal)
          readRecord[field.name] = fieldVal
        }
        return readRecord
      default:
        throw new Error(`Unknown type: ${fieldSchema.type}`)
    }
  }
}

/**
 * DatumWriter for generic python objects.
 */
class DatumWriter {
  constructor (writersSchema, options) {
    this.writersSchema = writersSchema
    this.options = options || {}
  }

  /**
   *
   * @param {*} datum
   * @param {Tap} tap
   *
   * @returns {Promise} Promise resolving when the write is finished
   */
  async write (datum, tap) {
    // validate datum
    if (!validate(this.writersSchema, datum, this.options)) {
      throw createAvroTypeError(this.writersSchema, datum)
    }

    await this.writeData(this.writersSchema, datum, tap)
  }

  /**
   *
   * @param {Schema} writersSchema
   * @param {*} datum
   * @param {Tap} tap
   */
  async writeData (writersSchema, datum, tap) {
    const logicalType = writersSchema.logicalType
    let datumToWrite = datum
    if (logicalType !== undefined && this.options.logicalTypes !== undefined && utils.isObject(this.options.logicalTypes[logicalType])) {
      const logicalTypeObject = this.options.logicalTypes[logicalType]
      datumToWrite = await logicalTypeObject.toValue(datum, writersSchema)
    }

    // function dispatch to write datum
    switch (writersSchema.type) {
      case 'null': break
      case 'boolean':
        await tap.writeBoolean(datumToWrite)
        break
      case 'string':
        await tap.writeString(datumToWrite)
        return
      case 'int':
        await tap.writeInt(datumToWrite)
        break
      case 'long':
        await tap.writeLong(datumToWrite)
        break
      case 'float':
        await tap.writeFloat(datumToWrite)
        break
      case 'double':
        await tap.writeDouble(datumToWrite)
        break
      case 'bytes':
        await tap.writeBytes(datumToWrite)
        break
      case 'fixed':
        await tap.writeFixed(datumToWrite, writersSchema.getProp('size'))
        break
      case 'enum':
        return this.writeEnum(writersSchema, datumToWrite, tap)
      case 'array':
        return this.writeArray(writersSchema, datumToWrite, tap)
      case 'map':
        return this.writeMap(writersSchema, datumToWrite, tap)
      case 'union':
      case 'error_union':
        return this.writeUnion(writersSchema, datumToWrite, tap)
      case 'record':
      case 'error':
      case 'request':
        return this.writeRecord(writersSchema, datumToWrite, tap)
      default:
        throw new Error(`Unknown type: ${writersSchema.type}`)
    }
  }

  /**
   * An enum is encoded by a int, representing the zero-based position
   * of the symbol in the schema.
   *
   * @param {Schema} writersSchema
   * @param {*} datum
   * @param {Tap} encoder
   */
  async writeEnum (writersSchema, datum, tap) {
    const indexOfDatum = writersSchema.symbols.indexOf(datum)
    tap.writeInt(indexOfDatum)
  }

  /**
   * Arrays are encoded as a series of blocks.
   *
   * Each block consists of a long count value,
   * followed by that many array items.
   * A block with count zero indicates the end of the array.
   * Each item is encoded per the array's item schema.
   *
   * If a block's count is negative,
   * then the count is followed immediately by a long block size,
   * indicating the number of bytes in the block.
   * The actual count in this case
   * is the absolute value of the count written.
   *
   * @param {Schema} writersSchema
   * @param {Array} datum
   * @param {Tap} tap
   */
  async writeArray (writersSchema, datum, tap) {
    if (datum.length > 0) {
      tap.writeLong(datum.length)
      for (const item of datum) {
        await this.writeData(writersSchema.items, item, tap)
      }
    }
    tap.writeLong(0)
  }

  /**
   * Maps are encoded as a series of blocks.
   *
   * Each block consists of a long count value,
   * followed by that many key/value pairs.
   * A block with count zero indicates the end of the map.
   * Each item is encoded per the map's value schema.
   *
   * If a block's count is negative,
   * then the count is followed immediately by a long block size,
   * indicating the number of bytes in the block.
   * The actual count in this case
   * is the absolute value of the count written.
   *
   * @param {Schema} writersSchema
   * @param {Object} datum
   * @param {Tap} tap
   */
  async writeMap (writersSchema, datum, tap) {
    const entries = Object.entries(datum)
    if (entries.length > 0) {
      tap.writeLong(entries.length)
      for (const [key, val] of entries) {
        tap.writeString(key)
        await this.writeData(writersSchema.values, val, tap)
      }
    }
    tap.writeLong(0)
  }

  /**
   * A union is encoded by first writing a long value indicating
   * the zero-based position within the union of the schema of its value.
   * The value is then encoded per the indicated schema within the union.
   *
   * @param {Schema} writersSchema
   * @param {Object} datum
   * @param {Tap} tap
   */
  async writeUnion (writersSchema, datum, tap) {
    // resolve union
    let indexOfSchema = -1
    for (let i = 0; i < writersSchema.schemas.length; i++) {
      const candidateSchema = writersSchema.schemas[i]
      if (validate(candidateSchema, datum, this.options)) {
        indexOfSchema = i
      }
    }

    if (indexOfSchema < 0) {
      throw createAvroTypeError(writersSchema, datum)
    }

    // write data
    tap.writeLong(indexOfSchema)
    await this.writeData(writersSchema.schemas[indexOfSchema], datum, tap)
  }

  /**
   * A record is encoded by encoding the values of its fields
   * in the order that they are declared. In other words, a record
   * is encoded as just the concatenation of the encodings of its fields.
   * Field values are encoded per their schema.
   *
   * @param {Schema} writersSchema
   * @param {Object} datum
   * @param {Tap} tap
   */
  async writeRecord (writersSchema, datum, tap) {
    for (const field of writersSchema.fields) {
      await this.writeData(field.type, datum[field.name], tap)
    }
  }
}

module.exports = {
  validate,
  DatumWriter,
  DatumReader
}
