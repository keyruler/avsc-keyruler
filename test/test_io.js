const io = require('../src/io')
const Tap = require('../src/tap')
const schema = require('../src/schema')
const assert = require('assert')

const SCHEMAS_TO_VALIDATE = [
  ['"null"', null],
  ['"boolean"', true],
  ['"string"', 'adsfasdf09809dsf-=adsf'],
  ['"bytes"', Buffer.from('12345abcd')],
  ['"int"', 1234],
  ['"long"', 1234],
  ['"float"', 1234.0],
  ['"double"', 1234.0],
  ['{"type": "fixed", "name": "Test", "size": 1}', Buffer.from('B')],
  ['{"type": "enum", "name": "Test", "symbols": ["A", "B"]}', 'B'],
  ['{"type": "array", "items": "long"}', [1, 3, 2]],
  ['{"type": "map", "values": "long"}', { a: 1, b: 3, c: 2 }],
  ['["string", "null", "long"]', null],
  [`
    {"type": "record",
    "name": "Test",
    "fields": [{"name": "f", "type": "long"}]}
   `, { f: 5 }
  ],
  [`
   {"type": "record",
    "name": "Lisp",
    "fields": [{"name": "value",
                "type": ["null", "string",
                         {"type": "record",
                          "name": "Cons",
                          "fields": [{"name": "car", "type": "Lisp"},
                                     {"name": "cdr", "type": "Lisp"}]}]}]}
   `, { value: { car: { value: 'head' }, cdr: { value: null } } }
  ]
]

const BINARY_ENCODINGS = [
  [0, '00'],
  [-1, '01'],
  [1, '02'],
  [-2, '03'],
  [2, '04'],
  [-64, '7f'],
  [64, '80 01'],
  [8192, '80 80 01'],
  [-8193, '81 80 01']
]

const DEFAULT_VALUE_EXAMPLES = [
  ['"null"', 'null', null],
  ['"boolean"', 'true', true],
  ['"string"', '"foo"', 'foo'],
  ['"bytes"', '"\u00FF\u00FF"', Buffer.from('\xff\xff')],
  ['"int"', '5', 5],
  ['"long"', '5', 5],
  ['"float"', '1.1', 1.1],
  ['"double"', '1.1', 1.1],
  ['{"type": "fixed", "name": "F", "size": 2}', '"\u00FF\u00FF"', Buffer.from('\xff\xff')],
  ['{"type": "enum", "name": "F", "symbols": ["FOO", "BAR"]}', '"FOO"', 'FOO'],
  ['{"type": "array", "items": "int"}', '[1, 2, 3]', [1, 2, 3]],
  ['{"type": "map", "values": "int"}', '{"a": 1, "b": 2}', { a: 1, b: 2 }],
  ['["int", "null"]', '5', 5],
  ['{"type": "record", "name": "F", "fields": [{"name": "A", "type": "int"}]}', '{"A": 5}', { A: 5 }]
]

const LONG_RECORD_SCHEMA = schema.parse(`
  {"type": "record",
   "name": "Test",
   "fields": [{"name": "A", "type": "int"},
              {"name": "B", "type": "int"},
              {"name": "C", "type": "int"},
              {"name": "D", "type": "int"},
              {"name": "E", "type": "int"},
              {"name": "F", "type": "int"},
              {"name": "G", "type": "int"}]}`)

const LONG_RECORD_DATUM = { A: 1, B: 2, C: 3, D: 4, E: 5, F: 6, G: 7 }

/**
 * Return the hex value, as a string, of a binary-encoded int or long.
 */
function avroHexlify (buffer) {
  const bytes = []
  let currentByte = buffer.toString('utf8', 0, 1)
  bytes.push(buffer.toString('hex', 0, 1))

  let i = 1
  while ((currentByte.codePointAt(0) & 0x80) !== 0) {
    const start = i
    const end = i + 1
    currentByte = buffer.toString('utf8', start, end)
    bytes.push(buffer.toString('hex', start, end))
    i = end
  }
  return bytes.join(' ')
}

async function writeDatum (datum, writersSchema) {
  const buffer = Buffer.alloc(1024)
  const tap = new Tap(buffer)
  const datumWriter = new io.DatumWriter(writersSchema)
  await datumWriter.write(datum, tap)
  return [buffer, tap, datumWriter]
}

async function readDatum (buffer, writersSchema, readersSchema) {
  const newBuffer = Buffer.from(buffer)
  const tap = new Tap(newBuffer)
  const datumReader = new io.DatumReader(writersSchema, readersSchema)
  return datumReader.read(tap)
}

async function checkBinaryEncoding (numberType) {
  let correct = 0
  for (const [datum, hexEncoding] of BINARY_ENCODINGS) {
    const writersSchema = schema.parse('"' + numberType.toLowerCase() + '"')
    const [buffer] = await writeDatum(datum, writersSchema)
    const hexVal = avroHexlify(buffer)
    if (hexEncoding === hexVal) {
      correct += 1
    }
  }
  return correct
}

async function checkSkipNumber (numberType) {
  let correct = 0
  for (const [valueToSkip] of BINARY_ENCODINGS) {
    const VALUE_TO_READ = 6253

    // write the value to skip and a known value
    const writersSchema = schema.parse('"' + numberType.toLowerCase() + '"')
    const [buffer, tap, datumWriter] = await writeDatum(valueToSkip, writersSchema)
    await datumWriter.write(VALUE_TO_READ, tap)

    // skip the value
    const readTap = new Tap(buffer)
    readTap.skipLong()

    // read data from string buffer
    const datumReader = new io.DatumReader(writersSchema)
    const readValue = await datumReader.read(readTap)

    if (readValue === VALUE_TO_READ) {
      correct += 1
    }
  }
  return correct
}

// TODO: All test which loop with different schemas should loop out different tests.
describe('IO', () => {
  describe('Basic functionality', () => {
    describe('Validate', () => {
      for (const [exampleSchema, datum] of SCHEMAS_TO_VALIDATE) {
        it(exampleSchema + ' => ' + JSON.stringify(datum), () => {
          const validated = io.validate(schema.parse(exampleSchema), datum)
          assert(validated)
        })
      }
    })

    describe('Round trip', () => {
      for (const [exampleSchema, datum] of SCHEMAS_TO_VALIDATE) {
        it(exampleSchema, async () => {
          const writersSchema = schema.parse(exampleSchema)
          const [buffer] = await writeDatum(datum, writersSchema)
          const roundTripDatum = await readDatum(buffer, writersSchema)

          assert.deepStrictEqual(datum, roundTripDatum)
        })
      }
    })
  })

  describe('Binary encoding of int and long', () => {
    it('Binary int encoding', async () => {
      const correct = await checkBinaryEncoding('int')
      assert.strictEqual(correct, BINARY_ENCODINGS.length)
    })

    it('Binary long encoding', async () => {
      const correct = await checkBinaryEncoding('long')
      assert.strictEqual(correct, BINARY_ENCODINGS.length)
    })

    it('Skip int', async () => {
      const correct = await checkSkipNumber('int')
      assert.strictEqual(correct, BINARY_ENCODINGS.length)
    })

    it('Skip long', async () => {
      const correct = await checkSkipNumber('long')
      assert.strictEqual(correct, BINARY_ENCODINGS.length)
    })
  })

  describe('Schema resolution', () => {
    it('Test schema promotion', async () => {
      // Note that checking writers_schema.type in read_data
      // allows us to handle promotion correctly
      const promotableSchemas = ['"int"', '"long"', '"float"', '"double"']
      let incorrect = 0
      for (let i = 0; i < promotableSchemas.length; i++) {
        const ws = promotableSchemas[i]
        const writersSchema = schema.parse(ws)
        const datumToWrite = 219
        for (const rs of promotableSchemas.slice(i + 1)) {
          const readersSchema = schema.parse(rs)
          const [buffer] = await writeDatum(datumToWrite, writersSchema)
          const datumRead = await readDatum(buffer, writersSchema, readersSchema)
          if (datumRead !== datumToWrite) {
            incorrect += 1
          }
        }
      }
      assert.strictEqual(incorrect, 0)
    })

    it('Test unknown symbol', async () => {
      const writersSchema = schema.parse(`
        {"type": "enum", "name": "Test",
        "symbols": ["FOO", "BAR"]}`)
      const datumToWrite = 'FOO'

      const readersSchema = schema.parse(`
        {"type": "enum", "name": "Test",
        "symbols": ["BAR", "BAZ"]}`)

      const [buffer] = await writeDatum(datumToWrite, writersSchema)
      const newTap = new Tap(buffer)
      const datumReader = new io.DatumReader(writersSchema, readersSchema)
      await assert.rejects(() => datumReader.read(newTap))
    })

    it('Test default values', async () => {
      const writersSchema = LONG_RECORD_SCHEMA
      const datumToWrite = LONG_RECORD_DATUM

      for (const [fieldType, defaultJson, defaultDatum] of DEFAULT_VALUE_EXAMPLES) {
        const readersSchema = schema.parse(`
          {"type": "record", "name": "Test",
           "fields": [{"name": "H", "type": ${fieldType}, "default": ${defaultJson}}]}`)
        const datumToRead = { H: defaultDatum }

        const [buffer] = await writeDatum(datumToWrite, writersSchema)
        const datumRead = await readDatum(buffer, writersSchema, readersSchema)
        assert.deepStrictEqual(datumRead, datumToRead)
      }
    })

    it('Test no default value', async () => {
      const writersSchema = LONG_RECORD_SCHEMA
      const datumToWrite = LONG_RECORD_DATUM

      const readersSchema = schema.parse(`
        {"type": "record", "name": "Test",
        "fields": [{"name": "H", "type": "int"}]}`)

      const [buffer] = await writeDatum(datumToWrite, writersSchema)
      const newTap = new Tap(buffer)
      const datumReader = new io.DatumReader(writersSchema, readersSchema)
      await assert.rejects(() => datumReader.read(newTap))
    })

    it('Test projection', async () => {
      const writersSchema = LONG_RECORD_SCHEMA
      const datumToWrite = LONG_RECORD_DATUM

      const readersSchema = schema.parse(`
        {"type": "record", "name": "Test",
        "fields": [{"name": "E", "type": "int"},
                    {"name": "F", "type": "int"}]}`)
      const datumToRead = { E: 5, F: 6 }

      const [buffer] = await writeDatum(datumToWrite, writersSchema)
      const datumRead = await readDatum(buffer, writersSchema, readersSchema)
      assert.deepStrictEqual(datumToRead, datumRead)
    })

    it('Test field order', async () => {
      const writersSchema = LONG_RECORD_SCHEMA
      const datumToWrite = LONG_RECORD_DATUM

      const readersSchema = schema.parse(`
        {"type": "record", "name": "Test",
        "fields": [{"name": "F", "type": "int"},
                    {"name": "E", "type": "int"}]}`)
      const datumToRead = { E: 5, F: 6 }

      const [buffer] = await writeDatum(datumToWrite, writersSchema)
      const datumRead = await readDatum(buffer, writersSchema, readersSchema)
      assert.deepStrictEqual(datumToRead, datumRead)
    })

    it('Test type exception', async () => {
      const writersSchema = schema.parse(`
        {"type": "record", "name": "Test",
        "fields": [{"name": "F", "type": "int"},
                    {"name": "E", "type": "int"}]}`)
      const datumToWrite = { E: 5, F: 'Bad' }
      await assert.rejects(() => writeDatum(datumToWrite, writersSchema))
    })
  })
})
