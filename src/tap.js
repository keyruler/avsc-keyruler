/*
Copyright (c) 2015-2017, Matthieu Monsch.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

/**
 * A tap is a buffer which remembers what has been already read.
 *
 * It is optimized for performance, at the cost of failing silently when
 * overflowing the buffer. This is a purposeful trade-off given the expected
 * rarity of this case and the large performance hit necessary to enforce
 * validity. See `isValid` below for more information.
 */
function Tap (buf, pos) {
  this.buf = buf
  this.pos = pos | 0
  if (this.pos < 0) {
    throw new Error('negative offset')
  }
}

/**
 * Check that the tap is in a valid state.
 *
 * For efficiency reasons, none of the methods below will fail if an overflow
 * occurs (either read, skip, or write). For this reason, it is up to the
 * caller to always check that the read, skip, or write was valid by calling
 * this method.
 */
Tap.prototype.isValid = function () { return this.pos <= this.buf.length }

// Read, skip, write methods.
//
// These should fail silently when the buffer overflows. Note this is only
// required to be true when the functions are decoding valid objects. For
// example errors will still be thrown if a bad count is read, leading to a
// negative position offset (which will typically cause a failure in
// `readFixed`).

Tap.prototype.readBoolean = function () { return !!this.buf[this.pos++] }

Tap.prototype.skipBoolean = function () { this.pos++ }

Tap.prototype.writeBoolean = function (b) { this.buf[this.pos++] = !!b }

Tap.prototype.readInt = Tap.prototype.readLong = function () {
  var n = 0
  var k = 0
  var buf = this.buf
  var b, h, f, fk

  do {
    b = buf[this.pos++]
    h = b & 0x80
    n |= (b & 0x7f) << k
    k += 7
  } while (h && k < 28)

  if (h) {
    // Switch to float arithmetic, otherwise we might overflow.
    f = n
    fk = 268435456 // 2 ** 28.
    do {
      b = buf[this.pos++]
      f += (b & 0x7f) * fk
      fk *= 128
    } while (b & 0x80)
    return (f % 2 ? -(f + 1) : f) / 2
  }

  return (n >> 1) ^ -(n & 1)
}

Tap.prototype.skipInt = Tap.prototype.skipLong = function () {
  var buf = this.buf
  while (buf[this.pos++] & 0x80) { }
}

Tap.prototype.writeInt = Tap.prototype.writeLong = function (n) {
  var buf = this.buf
  var f, m

  if (n >= -1073741824 && n < 1073741824) {
    // Won't overflow, we can use integer arithmetic.
    m = n >= 0 ? n << 1 : (~n << 1) | 1
    do {
      buf[this.pos] = m & 0x7f
      m >>= 7
    } while (m && (buf[this.pos++] |= 0x80))
  } else {
    // We have to use slower floating arithmetic.
    f = n >= 0 ? n * 2 : (-n * 2) - 1
    do {
      buf[this.pos] = f & 0x7f
      f /= 128
    } while (f >= 1 && (buf[this.pos++] |= 0x80))
  }
  this.pos++
}

Tap.prototype.readFloat = function () {
  var buf = this.buf
  var pos = this.pos
  this.pos += 4
  if (this.pos > buf.length) {
    return
  }
  return this.buf.readFloatLE(pos)
}

Tap.prototype.skipFloat = function () { this.pos += 4 }

Tap.prototype.writeFloat = function (f) {
  var buf = this.buf
  var pos = this.pos
  this.pos += 4
  if (this.pos > buf.length) {
    return
  }
  return this.buf.writeFloatLE(f, pos)
}

Tap.prototype.readDouble = function () {
  var buf = this.buf
  var pos = this.pos
  this.pos += 8
  if (this.pos > buf.length) {
    return
  }
  return this.buf.readDoubleLE(pos)
}

Tap.prototype.skipDouble = function () { this.pos += 8 }

Tap.prototype.writeDouble = function (d) {
  var buf = this.buf
  var pos = this.pos
  this.pos += 8
  if (this.pos > buf.length) {
    return
  }
  return this.buf.writeDoubleLE(d, pos)
}

Tap.prototype.readFixed = function (len) {
  var pos = this.pos
  this.pos += len
  if (this.pos > this.buf.length) {
    return
  }
  var fixed = Buffer.alloc(len)
  this.buf.copy(fixed, 0, pos, pos + len)
  return fixed
}

Tap.prototype.skipFixed = function (len) { this.pos += len }

Tap.prototype.writeFixed = function (buf, len) {
  len = len || buf.length
  var pos = this.pos
  this.pos += len
  if (this.pos > this.buf.length) {
    return
  }
  buf.copy(this.buf, pos, 0, len)
}

Tap.prototype.readBytes = function () {
  return this.readFixed(this.readLong())
}

Tap.prototype.skipBytes = function () {
  var len = this.readLong()
  this.pos += len
}

Tap.prototype.writeBytes = function (buf) {
  var len = buf.length
  this.writeLong(len)
  this.writeFixed(buf, len)
}

/* istanbul ignore else */
if (typeof Buffer.prototype.utf8Slice === 'function') {
  // Use this optimized function when available.
  Tap.prototype.readString = function () {
    var len = this.readLong()
    var pos = this.pos
    var buf = this.buf
    this.pos += len
    if (this.pos > buf.length) {
      return
    }
    return this.buf.utf8Slice(pos, pos + len)
  }
} else {
  Tap.prototype.readString = function () {
    var len = this.readLong()
    var pos = this.pos
    var buf = this.buf
    this.pos += len
    if (this.pos > buf.length) {
      return
    }
    return this.buf.slice(pos, pos + len).toString()
  }
}

Tap.prototype.skipString = function () {
  var len = this.readLong()
  this.pos += len
}

Tap.prototype.writeString = function (s) {
  var len = Buffer.byteLength(s)
  var buf = this.buf
  this.writeLong(len)
  var pos = this.pos
  this.pos += len
  if (this.pos > buf.length) {
    return
  }
  if (len > 64) {
    this._writeUtf8(s, len)
  } else {
    var i, l, c1, c2
    for (i = 0, l = len; i < l; i++) {
      c1 = s.charCodeAt(i)
      if (c1 < 0x80) {
        buf[pos++] = c1
      } else if (c1 < 0x800) {
        buf[pos++] = c1 >> 6 | 0xc0
        buf[pos++] = c1 & 0x3f | 0x80
      } else if (
        (c1 & 0xfc00) === 0xd800 &&
        ((c2 = s.charCodeAt(i + 1)) & 0xfc00) === 0xdc00
      ) {
        c1 = 0x10000 + ((c1 & 0x03ff) << 10) + (c2 & 0x03ff)
        i++
        buf[pos++] = c1 >> 18 | 0xf0
        buf[pos++] = c1 >> 12 & 0x3f | 0x80
        buf[pos++] = c1 >> 6 & 0x3f | 0x80
        buf[pos++] = c1 & 0x3f | 0x80
      } else {
        buf[pos++] = c1 >> 12 | 0xe0
        buf[pos++] = c1 >> 6 & 0x3f | 0x80
        buf[pos++] = c1 & 0x3f | 0x80
      }
    }
  }
}

/* istanbul ignore else */
if (typeof Buffer.prototype.utf8Write === 'function') {
  Tap.prototype._writeUtf8 = function (str, len) {
    this.buf.utf8Write(str, this.pos - len, len)
  }
} else {
  // `utf8Write` isn't available in the browser.
  Tap.prototype._writeUtf8 = function (str, len) {
    this.buf.write(str, this.pos - len, len, 'utf8')
  }
}

/* istanbul ignore else */
if (typeof Buffer.prototype.latin1Write === 'function') {
  // `binaryWrite` has been renamed to `latin1Write` in Node v6.4.0, see
  // https://github.com/nodejs/node/pull/7111. Note that the `'binary'`
  // encoding argument still works however.
  Tap.prototype.writeBinary = function (str, len) {
    var pos = this.pos
    this.pos += len
    if (this.pos > this.buf.length) {
      return
    }
    this.buf.latin1Write(str, pos, len)
  }
} else if (typeof Buffer.prototype.binaryWrite === 'function') {
  Tap.prototype.writeBinary = function (str, len) {
    var pos = this.pos
    this.pos += len
    if (this.pos > this.buf.length) {
      return
    }
    this.buf.binaryWrite(str, pos, len)
  }
} else {
  // Slowest implementation.
  Tap.prototype.writeBinary = function (s, len) {
    var pos = this.pos
    this.pos += len
    if (this.pos > this.buf.length) {
      return
    }
    this.buf.write(s, pos, len, 'binary')
  }
}

// Binary comparison methods.
//
// These are not guaranteed to consume the objects they are comparing when
// returning a non-zero result (allowing for performance benefits), so no other
// operations should be done on either tap after a compare returns a non-zero
// value. Also, these methods do not have the same silent failure requirement
// as read, skip, and write since they are assumed to be called on valid
// buffers.

Tap.prototype.matchBoolean = function (tap) {
  return this.buf[this.pos++] - tap.buf[tap.pos++]
}

Tap.prototype.matchInt = Tap.prototype.matchLong = function (tap) {
  var n1 = this.readLong()
  var n2 = tap.readLong()
  return n1 === n2 ? 0 : (n1 < n2 ? -1 : 1)
}

Tap.prototype.matchFloat = function (tap) {
  var n1 = this.readFloat()
  var n2 = tap.readFloat()
  return n1 === n2 ? 0 : (n1 < n2 ? -1 : 1)
}

Tap.prototype.matchDouble = function (tap) {
  var n1 = this.readDouble()
  var n2 = tap.readDouble()
  return n1 === n2 ? 0 : (n1 < n2 ? -1 : 1)
}

Tap.prototype.matchFixed = function (tap, len) {
  return this.readFixed(len).compare(tap.readFixed(len))
}

Tap.prototype.matchBytes = Tap.prototype.matchString = function (tap) {
  var l1 = this.readLong()
  var p1 = this.pos
  this.pos += l1
  var l2 = tap.readLong()
  var p2 = tap.pos
  tap.pos += l2
  var b1 = this.buf.slice(p1, this.pos)
  var b2 = tap.buf.slice(p2, tap.pos)
  return b1.compare(b2)
}

// Functions for supporting custom long classes.
//
// The two following methods allow the long implementations to not have to
// worry about Avro's zigzag encoding, we directly expose longs as unpacked.

Tap.prototype.unpackLongBytes = function () {
  var res = Buffer.alloc(8)
  var n = 0
  var i = 0 // Byte index in target buffer.
  var j = 6 // Bit offset in current target buffer byte.
  var buf = this.buf
  var b, neg

  b = buf[this.pos++]
  neg = b & 1
  res.fill(0)

  n |= (b & 0x7f) >> 1
  while (b & 0x80) {
    b = buf[this.pos++]
    n |= (b & 0x7f) << j
    j += 7
    if (j >= 8) {
      // Flush byte.
      j -= 8
      res[i++] = n
      n >>= 8
    }
  }
  res[i] = n

  if (neg) {
    invert(res, 8)
  }

  return res
}

Tap.prototype.packLongBytes = function (buf) {
  var neg = (buf[7] & 0x80) >> 7
  var res = this.buf
  var j = 1
  var k = 0
  var m = 3
  var n

  if (neg) {
    invert(buf, 8)
    n = 1
  } else {
    n = 0
  }

  var parts = [
    buf.readUIntLE(0, 3),
    buf.readUIntLE(3, 3),
    buf.readUIntLE(6, 2)
  ]
  // Not reading more than 24 bits because we need to be able to combine the
  // "carry" bits from the previous part and JavaScript only supports bitwise
  // operations on 32 bit integers.
  while (m && !parts[--m]) { } // Skip trailing 0s.

  // Leading parts (if any), we never bail early here since we need the
  // continuation bit to be set.
  while (k < m) {
    n |= parts[k++] << j
    j += 24
    while (j > 7) {
      res[this.pos++] = (n & 0x7f) | 0x80
      n >>= 7
      j -= 7
    }
  }

  // Final part, similar to normal packing aside from the initial offset.
  n |= parts[m] << j
  do {
    res[this.pos] = n & 0x7f
    n >>= 7
  } while (n && (res[this.pos++] |= 0x80))
  this.pos++

  // Restore original buffer (could make this optional?).
  if (neg) {
    invert(buf, 8)
  }
}

// Helpers.

/**
 * Invert all bits in a buffer.
 *
 * @param buf {Buffer} Non-empty buffer to invert.
 * @param len {Number} Buffer length (must be positive).
 */
function invert (buf, len) {
  while (len--) {
    buf[len] = ~buf[len]
  }
}

module.exports = Tap
