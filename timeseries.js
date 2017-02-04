'use strict'

var fs = require('fs')
var path = require('path')
var WithConn = require('with-conn-pg')
var Joi = require('joi')
var sql = require('sql')
var pump = require('pump')
var QueryStream = require('pg-query-stream')
var createTable = readQuery('create.sql')
var dropTable = readQuery('drop.sql')
var insertWithoutDate = readQuery('insert-without-date.sql')
var insertWithDate = readQuery('insert-with-date.sql')
var through = require('through2')

var schema = {
  id: Joi.number().positive(),
  asset: Joi.string().required(),
  value: Joi.number().required(),
  timestamp: Joi.date()
}

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
    joiSchema: schema,
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
    var valResult = Joi.validate(datapoint, schema)
    var toExec = insertWithoutDate

    if (valResult.error) {
      return callback(valResult.error)
    }

    datapoint = valResult.value

    var args = [
      datapoint.value,
      datapoint.asset
    ]

    if (datapoint.timestamp) {
      toExec = insertWithDate
      args.push(datapoint.timestamp)
    }

    conn.query(toExec, args, callback)
  }

  function returnFirst (result, callback) {
    callback(null, result ? result.rows[0] : null)
  }

  function createReadStream (opts) {
    var stream = through.obj()
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
        sqlDataPoint.timestamp.gte(opts.from)
      )
    }

    if (opts.to) {
      builder.where(
        sqlDataPoint.timestamp.lte(opts.to)
      )
    }

    var query = builder.toQuery()

    var qs = new QueryStream(query.text, query.values)

    pump(conn.query(qs), dest, done)
  }
}

module.exports = timeseries
