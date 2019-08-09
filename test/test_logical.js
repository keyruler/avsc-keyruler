const assert = require('assert')
const utils = require('../src/utils')
const schema = require('../src/schema')
const io = require('../src/io')
const Tap = require('../src/tap')

async function writeDatum (datum, writersSchema, options) {
  const buffer = Buffer.alloc(1024)
  const tap = new Tap(buffer)
  const datumWriter = new io.DatumWriter(writersSchema, options)
  await datumWriter.write(datum, tap)
  return [buffer.subarray(0, tap.pos), tap, datumWriter]
}

async function readDatum (buffer, writersSchema, readersSchema, options) {
  const newBuffer = Buffer.from(buffer)
  const tap = new Tap(newBuffer)
  const datumReader = new io.DatumReader(writersSchema, readersSchema, options)
  return datumReader.read(tap)
}

describe('Logical type', () => {
  it('Logical string add "H" at end when writing', async () => {
    const writersSchema = schema.parse('{"type": "string", "logicalType": "hello"}')
    const datumToWrite = 'Hello'

    const options = {
      logicalTypes: {
        hello: {
          toValue: (value, schema) => {
            return value + 'H'
          },
          validateBeforeToValue: (value, schema, options) => {
            return utils.isString(value)
          },
          fromValue: (value, schema) => {
            return value.slice(0, -1)
          },
          validateBeforeFromValue: (value, schema, options) => {
            return utils.isString(value)
          }
        }
      }
    }

    const [bufferNoLogical] = await writeDatum(datumToWrite, writersSchema)
    const [bufferLogical] = await writeDatum(datumToWrite, writersSchema, options)

    assert.notDeepStrictEqual(bufferLogical, bufferNoLogical)
    assert.strictEqual(bufferLogical[6], 0x48)

    const datumNoLogical = await readDatum(bufferNoLogical, writersSchema)
    const datumLogical = await readDatum(bufferLogical, writersSchema, undefined, options)

    assert.deepStrictEqual(datumNoLogical, datumLogical)
  })

  it('Logical save int as string', async () => {
    const writersSchema = schema.parse('{"type": "string", "logicalType": "hello"}')

    const options = {
      logicalTypes: {
        hello: {
          toValue: (value, schema) => {
            return value.toString()
          },
          validateBeforeToValue: (value, schema, options) => {
            return utils.isNumber(value)
          },
          fromValue: (value, schema) => {
            return Number.parseInt(value)
          },
          validateBeforeFromValue: (value, schema, options) => {
            return utils.isString(value)
          }
        }
      }
    }

    const [bufferNoLogical] = await writeDatum('1', writersSchema)
    const [bufferLogical] = await writeDatum(1, writersSchema, options)

    assert.deepStrictEqual(bufferLogical, bufferNoLogical)

    await assert.rejects(() => writeDatum('1', writersSchema, options))

    const datumNoLogical = await readDatum(bufferNoLogical, writersSchema)
    const datumLogical = await readDatum(bufferLogical, writersSchema, undefined, options)

    assert.notDeepStrictEqual(datumNoLogical, datumLogical)
  })

  it('Logical save int as string with delay', async () => {
    const writersSchema = schema.parse('{"type": "string", "logicalType": "hello"}')

    const options = {
      logicalTypes: {
        hello: {
          toValue: (value, schema) => {
            return new Promise((resolve) => {
              setTimeout(() => resolve(value.toString()), 500)
            })
          },
          validateBeforeToValue: (value, schema, options) => {
            return utils.isNumber(value)
          },
          fromValue: (value, schema) => {
            return new Promise((resolve) => {
              setTimeout(() => resolve(Number.parseInt(value)), 500)
            })
          },
          validateBeforeFromValue: (value, schema, options) => {
            return utils.isString(value)
          }
        }
      }
    }

    const [bufferNoLogical] = await writeDatum('1', writersSchema)
    const [bufferLogical] = await writeDatum(1, writersSchema, options)

    assert.deepStrictEqual(bufferLogical, bufferNoLogical)

    await assert.rejects(() => writeDatum('1', writersSchema, options))

    const datumNoLogical = await readDatum(bufferNoLogical, writersSchema)
    const datumLogical = await readDatum(bufferLogical, writersSchema, undefined, options)

    assert.notDeepStrictEqual(datumNoLogical, datumLogical)
  })
})
