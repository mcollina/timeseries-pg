'use strict'

var test = require('tape')
var build = require('./')
var withConn = require('with-conn-pg')
var Joi = require('joi')
var callback = require('callback-stream')

var connString = 'postgres://localhost/timeseries_test'
var schemaQuery = 'select column_name, data_type, character_maximum_length from INFORMATION_SCHEMA.COLUMNS where table_name = \'datapoints\' ORDER BY column_name'
var assets

test('create schema', function (t) {
  assets = build(connString)
  assets.dropSchema(function () {
    assets.createSchema(function (err) {
      t.error(err, 'no error')
      withConn(connString, function (conn, done) {
        t.error(err, 'no error')

        conn.query(schemaQuery, function (err, result) {
          t.error(err, 'no error')
          t.equal(result.rows.length, 4, 'has 3 columns')
          t.equal(result.rows[0].column_name, 'asset', 'has an asset id')
          t.equal(result.rows[1].column_name, 'id', 'has an id')
          t.equal(result.rows[2].column_name, 'timestamp', 'has a date')
          t.equal(result.rows[3].column_name, 'value', 'has a value')
          done()
        })
      })(function (err) {
        t.error(err, 'no error')
        withConn.end()
        t.end()
      })
    })
  })
})

test('can insert a data point', function (t) {
  var expected = {
    value: 42.42,
    asset: 'anassetid'
  }
  assets.put(expected, function (err, result) {
    t.error(err, 'no error')
    t.ok(result.id, 'it has an id')
    t.ok(result.timestamp, 'it has a timestamp')
    t.ok(result.timestamp instanceof Date, 'timestamp is a Date')
    delete result.id
    delete result.timestamp
    t.deepEqual(result, expected, 'matches')
    withConn.end()
    t.end()
  })
})

test('can insert a data point with a timestamp', function (t) {
  var expected = {
    value: 42.42,
    asset: 'anassetid',
    timestamp: new Date()
  }
  assets.put(expected, function (err, result) {
    t.error(err, 'no error')
    t.equal(expected.timestamp.getTime(), result.timestamp.getTime(), 'time matches')
    delete result.id
    delete result.timestamp
    delete expected.timestamp
    t.deepEqual(result, expected, 'matches')
    withConn.end()
    t.end()
  })
})

test('cannot insert an asset without a value', function (t) {
  var expected = {
    asset: 'anassetid'
  }
  assets.put(expected, function (err, result) {
    t.ok(err, 'insert errors')
    t.equal(err.name, 'ValidationError', 'error type matches')
    t.equal(err.details[0].message, '"value" is required', 'validation error matches')
    withConn.end()
    t.end()
  })
})

test('cannot insert an asset without an asset', function (t) {
  var expected = {
    value: 42
  }
  assets.put(expected, function (err, result) {
    t.ok(err, 'insert errors')
    t.equal(err.name, 'ValidationError', 'error type matches')
    t.equal(err.details[0].message, '"asset" is required', 'validation error matches')
    withConn.end()
    t.end()
  })
})

test('can get datapoints', function (t) {
  var toWrite = {
    value: 42,
    asset: 'myasset'
  }
  assets.put(toWrite, function (err, expected) {
    t.error(err, 'no error')
    assets.createReadStream({
      asset: 'myasset'
    }).pipe(callback({ objectMode: true }, function (err, results) {
      t.error(err, 'no error')
      t.deepEqual(results, [expected], 'matches')
      withConn.end()
      t.end()
    }))
  })
})

test('can get datapoints in an interval', function (t) {
  var toWrite1 = {
    value: 42,
    asset: 'myasset',
    timestamp: new Date(Date.parse('1984-06-26'))
  }
  var toWrite2 = {
    value: 24,
    asset: 'myasset',
    timestamp: new Date(Date.parse('2003-06-26'))
  }
  var toWrite3 = {
    value: 42,
    asset: 'myasset',
    timestamp: new Date(Date.parse('2015-06-26'))
  }
  assets.put(toWrite1, function (err, expected1) {
    t.error(err, 'no error')
    assets.put(toWrite2, function (err, expected2) {
      t.error(err, 'no error')
      assets.put(toWrite3, function (err, expected3) {
        t.error(err, 'no error')

        assets.createReadStream({
          asset: 'myasset',
          from: new Date(Date.parse('2000-06-26')),
          to: new Date(Date.parse('2005-06-26'))
        }).pipe(callback({ objectMode: true }, function (err, results) {
          t.error(err, 'no error')
          t.deepEqual(results, [expected2], 'matches')
          withConn.end()
          t.end()
        }))
      })
    })
  })
})
