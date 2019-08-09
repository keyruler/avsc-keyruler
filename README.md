# Avsc - keyruler version [![Sponsor](https://johndoeinvest.com/logo-jdi-tag.png)](https://johndoeinvest.com/) [![Build status](https://api.travis-ci.com/keyruler/avsc-keyruler.svg?branch=master)](https://travis-ci.com/keyruler/avsc-keyruler)

This is a Avro schema parser and serializer based on the avro python library. The buffer encoding is from the [avsc](https://github.com/mtth/avsc) library.

This library is different from avsc since it allows async (de)serialization with logical types.

## Why are the default logical types not implemented?
Avro has some default logical types which have not been implemented partly because of limitaions with JS and partly because time/date is not standardized enough in JS.