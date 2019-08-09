const constants = require('../src/constants')
const schema = require('../src/schema')
const utils = require('../src/utils')

const assert = require('assert')

function makeExampleSchema (schemaString, valid, name = '', comment = '') {
  return { schema: schemaString, valid, name, comment }
}

function makePrimitiveExamples () {
  const examples = []
  for (const type of constants.PRIMITIVE_TYPES) {
    examples.push(makeExampleSchema(`"${type}"`, true))
    examples.push(makeExampleSchema(`{"type": "${type}"}`, true))
  }
  return examples
}

const PRIMITIVE_EXAMPLES = [
  makeExampleSchema('"true"', false),
  makeExampleSchema('true', false),
  makeExampleSchema('{"no_type": "test"}', false),
  makeExampleSchema('{"type": "panther"}', false),
  ...makePrimitiveExamples()
]

const FIXED_EXAMPLES = [
  makeExampleSchema('{"type": "fixed", "name": "Test", "size": 1}', true),
  makeExampleSchema(`{"type": "fixed",
     "name": "MyFixed",
     "namespace": "org.apache.hadoop.avro",
     "size": 1}
     `, true),
  makeExampleSchema(`
    {"type": "fixed",
     "name": "Missing size"}
    `, false),
  makeExampleSchema(`
    {"type": "fixed",
     "size": 314}
    `, false)
]

const ENUM_EXAMPLES = [
  makeExampleSchema('{"type": "enum", "name": "Test", "symbols": ["A", "B"]}', true),
  makeExampleSchema(`
    {"type": "enum",
     "name": "Status",
     "symbols": "Normal Caution Critical"}
    `, false),
  makeExampleSchema(`
    {"type": "enum",
     "name": [ 0, 1, 1, 2, 3, 5, 8 ],
     "symbols": ["Golden", "Mean"]}
    `, false),
  makeExampleSchema(`
    {"type": "enum",
     "symbols" : ["I", "will", "fail", "no", "name"]}
    `, false),
  makeExampleSchema(`
    {"type": "enum",
     "name": "Test"
     "symbols" : ["AA", "AA"]}
    `, false)
]

const ARRAY_EXAMPLES = [
  makeExampleSchema('{"type": "array", "items": "long"}', true),
  makeExampleSchema(`
    {"type": "array",
     "items": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}}
    `, true)
]

const MAP_EXAMPLES = [
  makeExampleSchema('{"type": "map", "values": "long"}', true),
  makeExampleSchema(`
    {"type": "map",
     "values": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}}
    `, true)
]

const UNION_EXAMPLES = [
  makeExampleSchema('["string", "null", "long"]', true),
  makeExampleSchema('["null", "null"]', false),
  makeExampleSchema('["long", "long"]', false),
  makeExampleSchema(`
    [{"type": "array", "items": "long"}
     {"type": "array", "items": "string"}]
    `, false)
]

const RECORD_EXAMPLES = [
  makeExampleSchema(`
    {"type": "record",
     "name": "Test",
     "fields": [{"name": "f",
                 "type": "long"}]}
    `, true),
  makeExampleSchema(`
    {"type": "error",
     "name": "Test",
     "fields": [{"name": "f",
                 "type": "long"}]}
    `, true),
  makeExampleSchema(`
    {"type": "record",
     "name": "Node",
     "fields": [{"name": "label", "type": "string"},
                {"name": "children",
                 "type": {"type": "array", "items": "Node"}}]}
    `, true),
  makeExampleSchema(`
    {"type": "record",
     "name": "Lisp",
     "fields": [{"name": "value",
                 "type": ["null", "string",
                          {"type": "record",
                           "name": "Cons",
                           "fields": [{"name": "car", "type": "Lisp"},
                                      {"name": "cdr", "type": "Lisp"}]}]}]}
    `, true),
  makeExampleSchema(`
    {"type": "record",
     "name": "HandshakeRequest",
     "namespace": "org.apache.avro.ipc",
     "fields": [{"name": "clientHash",
                 "type": {"type": "fixed", "name": "MD5", "size": 16}},
                {"name": "clientProtocol", "type": ["null", "string"]},
                {"name": "serverHash", "type": "MD5"},
                {"name": "meta", 
                 "type": ["null", {"type": "map", "values": "bytes"}]}]}
    `, true),
  makeExampleSchema(`
    {"type": "record",
     "name": "HandshakeResponse",
     "namespace": "org.apache.avro.ipc",
     "fields": [{"name": "match",
                 "type": {"type": "enum",
                          "name": "HandshakeMatch",
                          "symbols": ["BOTH", "CLIENT", "NONE"]}},
                {"name": "serverProtocol", "type": ["null", "string"]},
                {"name": "serverHash",
                 "type": ["null",
                          {"name": "MD5", "size": 16, "type": "fixed"}]},
                {"name": "meta",
                 "type": ["null", {"type": "map", "values": "bytes"}]}]}
    `, true),
  makeExampleSchema(`
    {"type": "record",
     "name": "Interop",
     "namespace": "org.apache.avro",
     "fields": [{"name": "intField", "type": "int"},
                {"name": "longField", "type": "long"},
                {"name": "stringField", "type": "string"},
                {"name": "boolField", "type": "boolean"},
                {"name": "floatField", "type": "float"},
                {"name": "doubleField", "type": "double"},
                {"name": "bytesField", "type": "bytes"},
                {"name": "nullField", "type": "null"},
                {"name": "arrayField",
                 "type": {"type": "array", "items": "double"}},
                {"name": "mapField",
                 "type": {"type": "map",
                          "values": {"name": "Foo",
                                     "type": "record",
                                     "fields": [{"name": "label",
                                                 "type": "string"}]}}},
                {"name": "unionField",
                 "type": ["boolean",
                          "double",
                          {"type": "array", "items": "bytes"}]},
                {"name": "enumField",
                 "type": {"type": "enum",
                          "name": "Kind",
                          "symbols": ["A", "B", "C"]}},
                {"name": "fixedField",
                 "type": {"type": "fixed", "name": "MD5", "size": 16}},
                {"name": "recordField",
                 "type": {"type": "record",
                          "name": "Node",
                          "fields": [{"name": "label", "type": "string"},
                                     {"name": "children",
                                      "type": {"type": "array",
                                               "items": "Node"}}]}}]}
    `, true),
  makeExampleSchema(`
    {"type": "record",
     "name": "ipAddr",
     "fields": [{"name": "addr", 
                 "type": [{"name": "IPv6", "type": "fixed", "size": 16},
                          {"name": "IPv4", "type": "fixed", "size": 4}]}]}
    `, true),
  makeExampleSchema(`
    {"type": "record",
     "name": "Address",
     "fields": [{"type": "string"},
                {"type": "string", "name": "City"}]}
    `, false),
  makeExampleSchema(`
    {"type": "record",
     "name": "Event",
     "fields": [{"name": "Sponsor"},
                {"name": "City", "type": "string"}]}
    `, false),
  makeExampleSchema(`
    {"type": "record",
     "fields": "His vision, from the constantly passing bars,"
     "name", "Rainer"}
    `, false),
  makeExampleSchema(`
    {"name": ["Tom", "Jerry"],
     "type": "record",
     "fields": [{"name": "name", "type": "string"}]}
    `, false)
]

const DOC_EXAMPLES = [
  makeExampleSchema(`
    {"type": "record",
     "name": "TestDoc",
     "doc":  "Doc string",
     "fields": [{"name": "name", "type": "string", 
                 "doc" : "Doc String"}]}
    `, true),
  makeExampleSchema(`
    {"type": "enum", "name": "Test", "symbols": ["A", "B"],
     "doc": "Doc String"}
    `, true)
]

const OTHER_PROP_EXAMPLES = [
  makeExampleSchema(`
    {"type": "record",
     "name": "TestRecord",
     "cp_string": "string",
     "cp_int": 1,
     "cp_array": [ 1, 2, 3, 4],
     "fields": [ {"name": "f1", "type": "string", "cp_object": {"a":1,"b":2} },
                 {"name": "f2", "type": "long", "cp_null": null} ]}
    `, true),
  makeExampleSchema(`
     {"type": "map", "values": "long", "cp_boolean": true}
    `, true),
  makeExampleSchema(`
    {"type": "enum",
     "name": "TestEnum",
     "symbols": [ "one", "two", "three" ],
     "cp_float" : 1.0 }
    `, true),
  makeExampleSchema(`
    {"type": "long",
     "date": "true"}
    `, true)
]

const EXAMPLES = PRIMITIVE_EXAMPLES.concat(
  FIXED_EXAMPLES,
  ENUM_EXAMPLES,
  ARRAY_EXAMPLES,
  MAP_EXAMPLES,
  UNION_EXAMPLES,
  RECORD_EXAMPLES,
  DOC_EXAMPLES
)

const VALID_EXAMPLES = EXAMPLES.filter(e => e.valid)

describe('Schema', () => {
  it('Correct recursive extraction', () => {
    const s = schema.parse('{"type": "record", "name": "X", "fields": [{"name": "y", "type": {"type": "record", "name": "Y", "fields": [{"name": "Z", "type": "X"}]}}]}')
    const t = schema.parse(s.fields[0].type.toString())
    // If we've made it this far, the subschema was reasonably stringified; it ccould be reparsed.
    assert.strictEqual('X', t.fields[0].type.name)
  })

  describe('parse', () => {
    for (const example of EXAMPLES) {
      it(example.schema, () => {
        const fn = example.valid ? assert.doesNotThrow : assert.throws
        return fn(() => schema.parse(example.schema))
      })
    }
  })

  /**
   * Test that the string generated by an Avro Schema object
   * is, in fact, a valid Avro schema.
   */
  describe('Valid cast to string after parse', () => {
    for (const example of VALID_EXAMPLES) {
      it(example.schema, () => {
        const schemaData = schema.parse(example.schema)
        assert.doesNotThrow(() => schema.parse(schemaData.toString()))
      })
    }
  })

  /**
   * 1. Given a string, parse it to get Avro schema "original".
   * 2. Serialize "original" to a string and parse that string to generate Avro schema "round trip".
   * 3. Ensure "original" and "round trip" schemas are equivalent.
   */
  describe('Equivalence after round trip', () => {
    for (const example of VALID_EXAMPLES) {
      it(example.schema, () => {
        const originalSchema = schema.parse(example.schema)
        const roundTripSchema = schema.parse(originalSchema.toString())
        assert(roundTripSchema.equals(originalSchema))
      })
    }
  })

  /**
   * The fullname is determined in one of the following ways:
   *  * A name and namespace are both specified.  For example,
   *    one might use "name": "X", "namespace": "org.foo"
   *     to indicate the fullname "org.foo.X".
   *   * A fullname is specified.  If the name specified contains
   *     a dot, then it is assumed to be a fullname, and any
   *     namespace also specified is ignored.  For example,
   *     use "name": "org.foo.X" to indicate the
   *     fullname "org.foo.X".
   *   * A name only is specified, i.e., a name that contains no
   *     dots.  In this case the namespace is taken from the most
   *     tightly encosing schema or protocol.  For example,
   *     if "name": "X" is specified, and this occurs
   *     within a field of the record definition
   *     of "org.foo.Y", then the fullname is "org.foo.X".
   *
   *  References to previously defined names are as in the latter
   *  two cases above: if they contain a dot they are a fullname, if
   *  they do not contain a dot, the namespace is the namespace of
   *  the enclosing definition.
   *
   *  Primitive type names have no namespace and their names may
   *  not be defined in any namespace.  A schema may only contain
   *  multiple definitions of a fullname if the definitions are
   *  equivalent.
   */
  describe('Fullname', () => {
    it('name and namespace specified', () => {
      const fullname = new schema.Name('a', 'o.a.h', undefined).fullname
      assert.strictEqual(fullname, 'o.a.h.a')
    })

    it('fullname and namespace specified', () => {
      const fullname = new schema.Name('a.b.c.d', 'o.a.h', undefined).fullname
      assert.strictEqual(fullname, 'a.b.c.d')
    })

    it('name and default namespace specified', () => {
      const fullname = new schema.Name('a', undefined, 'b.c.d').fullname
      assert.strictEqual(fullname, 'b.c.d.a')
    })

    it('fullname and default namespace specified', () => {
      const fullname = new schema.Name('a.b.c.d', undefined, 'o.a.h').fullname
      assert.strictEqual(fullname, 'a.b.c.d')
    })

    it('fullname, namespace, default namespace specified', () => {
      const fullname = new schema.Name('a.b.c.d', 'o.a.a', 'o.a.h').fullname
      assert.strictEqual(fullname, 'a.b.c.d')
    })

    it('name, namespace, default namespace specified', () => {
      const fullname = new schema.Name('a', 'o.a.a', 'o.a.h').fullname
      assert.strictEqual(fullname, 'o.a.a.a')
    })
  })

  describe('Doc attributes', () => {
    for (const example of DOC_EXAMPLES) {
      it(example.schema, () => {
        const originalSchema = schema.parse(example.schema)
        assert(originalSchema.doc !== undefined)
        if (originalSchema.type === 'record') {
          for (const f of originalSchema.fields) {
            assert(f.doc !== undefined)
          }
        }
      })
    }
  })

  describe('Other attributes', () => {
    const props = {}
    for (const example of OTHER_PROP_EXAMPLES) {
      it(example.schema, () => {
        const originalSchema = schema.parse(example.schema)
        const roundTripSchema = schema.parse(originalSchema.toString())
        assert(utils.isEqual(originalSchema.otherProps, roundTripSchema.otherProps))

        if (originalSchema.type === 'record') {
          let fieldProps = 0
          for (const f of originalSchema.fields) {
            if (f.otherProps !== undefined) {
              // props.update(f.other_props)
              fieldProps += 1
            }
          }
          assert.strictEqual(fieldProps, originalSchema.fields.length)
        }

        assert(originalSchema.otherProps !== undefined)
        // props.update(original_schema.other_props)
      })
    }

    it('Test props', () => {
      for (const [k, v] of Object.entries(props)) {
        if (k === 'cp_boolean') {
          assert.strictEqual(typeof v, 'boolean')
        } else if (k === 'cp_int') {
          assert.strictEqual(typeof v, 'number')
        } else if (k === 'cp_object') {
          assert(utils.isObject(v))
        } else if (k === 'cp_float') {
          assert.strictEqual(typeof v, 'number')
        } else if (k === 'cp_array') {
          assert(Array.isArray(v))
        }
      }
    })
  })

  describe('Exception is not swallowed on parse error', () => {
    let caughtException
    try {
      schema.parse('/not/a/real/file')
      caughtException = false
    } catch (e) {
      const expectedMessage = 'Error parsing JSON: /not/a/real/file, error = SyntaxError: Unexpected token / in JSON at position 0'
      assert.strictEqual(expectedMessage, e.message)
      caughtException = true
    }

    assert(caughtException, 'Exception was not caught')
  })
})
