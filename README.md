# timeseries-pg

Manage timeseries, with node and postgres

An datapoint can be in three states: `'wait'`, `'operational'` and
`'error'`.

## Install

```
npm install @matteo.collina/timeseries-pg --save
```

<a name="api"></a>
## API

  * <a href="#timeseries"><code><b>buildTimeseries()</b></code></a>
  * <a href="#put"><code>timeseries.<b>put()</b></code></a>
  * <a
    href="#createReadStream"><code>timeseries.<b>createReadStream()</b></code></a>
  * <a href="#createSchema"><code>timeseries.<b>createSchema()</b></code></a>
  * <a href="#dropSchema"><code>timeseries.<b>dropSchema()</b></code></a>

-------------------------------------------------------

<a name="timeseries"></a>
### buildtimeseries(connectionString)

The factory for the timeseries module, you can just pass through a
[pg](http:/npm.im/pg) connection string.

Example:

```js
var connString = 'postgres://localhost/timeseries_tests'
var timeseries = require('@matteo.collina/timeseries-pg')(connString)
```

-------------------------------------------------------

<a name="put"></a>
### timeseries.put(object, callback(err, datapoint))

Adds or updates an datapoint. An datapoint can have three properties:

1. the `'id'`, which needs to be set only for existing datapoints
2. the `'asset'`, the asset from which this datapoint was acquired
3. the `'value'`, a double that is the core of the datapoint
4. the `'timestamp'`, which defaults to `Date.now()`

Validation is provided by [Joi](http://npm.im/joi), and a Joi error
object will be provided in case of validation errors.

The returned datapoint includes the `id` and `timestamp`, if missing.

-------------------------------------------------------

<a name="createReadStream"></a>
### timeseries.createReadStream(opts)

Returns a
[Readable Stream](https://nodejs.org/api/stream.html#stream_class_stream_readable) that returns the data points.

Acceptable options are:

* `asset`: specify the asset that generated the datapoint
* `from`: the minimum timestamp that will be considered, it must be a `Date` object
* `to`: the maximum timestamp that will be considered, it must be a `Date` object

-------------------------------------------------------

<a name="createSchema"></a>
### timeseries.createSchema(callback(err))

Create the schema in PostgreSQL for this module.

-------------------------------------------------------

<a name="dropSchema"></a>
### timeseries.dropSchema(callback(err))

Drop the schema in PostgreSQL for this module.

## License

MIT
