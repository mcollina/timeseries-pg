'use strict'

var fs = require('fs')
var path = require('path')
var WithConn = require('with-conn-pg')
var createError = require('http-errors')
var Ajv = require('ajv')
var ajv = new Ajv()
var sql = require('sql')
var pump = require('pump')
var QueryStream = require('pg-query-stream')
var createTable = readQuery('create.sql')
var dropTable = readQuery('drop.sql')
var insertWithoutDate = readQuery('insert-without-date.sql')
var insertWithDate = readQuery('insert-with-date.sql')
var through = require('through2')

var schema = {
  type: 'object',
  required: ['asset', 'value'],
  properties: {
    id: {
      type: 'number'
    },
    asset: {
      type: 'string',
      minLength: 1
    },
    value: {
      type: 'number'
    },
    timestamp: {
      type: 'string',
      strFormat: 'date-time'
    }
  }
}

var validate = ajv.compile(schema)

var sqlDataPoint = sql.define({
  name: 'datapoints',
  columns: ['id', 'value', 'asset', 'timestamp']
})

function readQuery (file) {
  return fs.readFileSync(path.join(__dirname, 'sql', file), 'utf8')
}

function timeseries (connString) {
  var withConn = WithConn(connString)
  var pipeReadStream = withConn(_createReadStream)

  return {
    jsonSchema: schema,
    createSchema: withConn(createSchema),
    dropSchema: withConn(dropSchema),
    put: withConn([
      execPut,
      returnFirst
    ]),
    createReadStream: createReadStream,
    end: withConn.end.bind(withConn)
  }

  function createSchema (conn, callback) {
    conn.query(createTable, callback)
  }

  function dropSchema (conn, callback) {
    conn.query(dropTable, callback)
  }

  function execPut (conn, datapoint, callback) {
    var valid = validate(datapoint)
    var toExec = insertWithoutDate

    if (!valid) {
      var err = new createError.UnprocessableEntity()
      err.details = validate.errors
      return callback(err)
    }

    var args = [
      datapoint.value,
      datapoint.asset
    ]

    if (datapoint.timestamp) {
      toExec = insertWithDate
      args.push(new Date(datapoint.timestamp))
    }

    conn.query(toExec, args, callback)
  }

  function returnFirst (result, callback) {
    var value
    if (result) {
      value = result.rows[0]
      value.timestamp = value.timestamp.toISOString()
    }
    callback(null, value)
  }

  function createReadStream (opts) {
    var stream = through.obj(function (chunk, enc, cb) {
      chunk.timestamp = chunk.timestamp.toISOString()
      cb(null, chunk)
    })
    pipeReadStream(stream, opts, function (err) {
      if (err) {
        stream.emit('error', err)
      }
    })
    return stream
  }

  function _createReadStream (conn, dest, opts, done) {
    var builder = sqlDataPoint
      .select(sqlDataPoint.star())
      .from(sqlDataPoint)

    if (opts.asset) {
      builder.where(
        sqlDataPoint.asset.equals(opts.asset)
      )
    }

    if (opts.from) {
      builder.where(
        sqlDataPoint.timestamp.gte(new Date(opts.from))
      )
    }

    if (opts.to) {
      builder.where(
        sqlDataPoint.timestamp.lte(new Date(opts.to))
      )
    }

    var query = builder.toQuery()

    var qs = new QueryStream(query.text, query.values)

    pump(conn.query(qs), dest, done)
  }
}

module.exports = timeseries
