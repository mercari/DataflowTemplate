# Spanner Source Module

Source Module for loading data by specifying a query or table into Cloud Spanner.

## Source module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `spanner` |
| schema | - | [Schema](SCHEMA.md) | Schema of the data to be read. spanner module does not require specification |
| microbatch | Boolean | optional | Specify true if you want to retrieve data in near real time using the Micorobatch method. Default is false. (You need to start Dataflow in streaming mode if microbatch mode) |
| parameters | required | Map<String,Object\> | Specify the following individual parameters |

## Spanner source module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| projectId | required | String | The GCP Project ID of the Spanner you want to load |
| instanceId | required | String | The Instance ID of the Spanner you want to load |
| databaseId | required | String | The Database ID of the Spanner you want to load |
| query | selective required | String | Specify the SQL to read data from Spanner. Not necessary if table is specified. You can also specify the path of the GCS where you put the SQL file. |
| table | selective required | String | Specify the table name to read data from Spanner. Not necessary if query is specified. |
| fields | optional | Array<String\> | Specify the name of the field you want to read from the table. The default is all fields. |
| timestampBound | optional | String | Specify when you want to read the data at the specified time. Format: `yyyy-MM-ddTHH:mm:SSZ` |

### Spanner source module parameters for microbatch mode

(Microbatch mode is in experimental)

| parameter | optional | type | description |
| --- | --- | --- | --- |
| intervalSecond | optional | Integer | The interval at which the query is executed. Default is 60 seconds. |
| gapSecond | optional | Integer | Buffer time to allow a margin between the end of the time range of the query and the current time. Default is 30 seconds. |
| maxDurationMinute | optional | Integer | Maximum time range for the query. This value is used if the interval between the start of the time range of the query and the current time is greater than intervalSecond. Default is 60 minutes. |
| catchupIntervalSecond | optional | Integer | The interval at which the query will be executed if the interval between the query start time and the current time is large. The unit is seconds. Default is the same as intervalSecond. |
| startDatetime | optional | String | Start time of the first query. If not set, the value of outputCheckpoint will be used. |
| outputCheckpoint | optional | String | Specify the GCS path if you want to record the latest time when the query was executed in GCS. |

For more information about microbatch mode parameters, please refer to [microbatch page](microbatch.md).

## Related example config files

* [Cloud Spanner to BigQuery](../../../../examples/spanner-to-bigquery.json)
* [Cloud Spanner to Cloud Storage(Avro)](../../../../examples/spanner-to-avro.json)
* [Cloud Spanner to Cloud Datastore](../../../../examples/spanner-to-datastore.json)
* [Cloud Spanner to Cloud SQL](../../../../examples/spanner-to-jdbc.json)
* [Cloud Spanner to Cloud Spanner(Insert)](../../../../examples/spanner-to-spanner.json)
* [Cloud Spanner to Cloud Spanner(Delete)](../../../../examples/spanner-to-spanner-delete.json)
* [Cloud Spanner microbatch to BigQuery](../../../../examples/spanner-microbatch-to-bigquery.json)
