# Spanner Source Module

Source Module for loading data by specifying a query or table into Cloud Spanner.

## Source module common parameters

| parameter          | optional | type                | description                                                                                                                                                                   |
|--------------------|----------|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| name               | required | String              | Step name. specified to be unique in config file.                                                                                                                             |
| module             | required | String              | Specified `spanner`                                                                                                                                                           |
| schema             | -        | [Schema](SCHEMA.md) | Schema of the data to be read. spanner module does not require specification                                                                                                  |
| microbatch         | optional | Boolean             | Specify true if you want to retrieve data in near real time using the Micorobatch method. Default is false. (You need to start Dataflow in streaming mode if microbatch mode) |
| timestampAttribute | optional | String              | If you want to use the value of an field as the event time, specify the name of the field. (The field must be Timestamp or Date type)                                         |
| parameters         | required | Map<String,Object\> | Specify the following individual parameters                                                                                                                                   |

## Spanner source module parameters

| parameter       | optional           | type           | description                                                                                                                                                                                                  |
|-----------------|--------------------|----------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| projectId       | required           | String         | The GCP Project ID of the Spanner you want to load                                                                                                                                                           |
| instanceId      | required           | String         | The Instance ID of the Spanner you want to load                                                                                                                                                              |
| databaseId      | required           | String         | The Database ID of the Spanner you want to load                                                                                                                                                              |
| query           | selective required | String         | Specify the SQL to read data from Spanner. Not necessary if table is specified. You can also specify the path of the GCS where you put the SQL file.                                                         |
| table           | selective required | String         | Specify the table name to read data from Spanner. Not necessary if query is specified.                                                                                                                       |
| fields          | optional           | Array<String\> | Specify the name of the field you want to read from the table. The default is all fields.                                                                                                                    |
| timestampBound  | optional           | String         | Specify when you want to read the data at the specified time. Format: `yyyy-MM-ddTHH:mm:SSZ`                                                                                                                 |
| priority        | optional           | Enum           | Specify either `HIGH`, `MEDIUM`, or `LOW` as the query [priority](https://cloud.google.com/spanner/docs/cpu-utilization) to Spanner. The default is `MEDIUM`.                                                |
| enableDataBoost | optional           | Boolean        | Specify to enable [data boost](https://cloud.google.com/spanner/docs/databoost/databoost-overview). The default is false.                                                                                    |
| mode            | optional           | Enum           | Specify execution mode either `batch`, `changestream`, or `microbatch`. The default is `batch` when batch mode, `microbatch` when streaming mode and `microbatch` is true, `changestream` in the other case. |
| requestTag      | optional           | String         | Specify the [request tag](https://cloud.google.com/spanner/docs/introspection/troubleshooting-with-tags#request_tags) to be given to the query to Spanner.                                                   |

### Spanner source module parameters for changestream mode

(ChangeStream mode is in experimental)

To run in `changestream` mode, specify `streaming=true`, `additional-experiments=use_runner_v2` parameters at runtime.

| parameter          | optional | type                | description                                                                                                                                                  |
|--------------------|----------|---------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| changeStreamName   | required | String              | Specify name of the spanner change stream to read from.                                                                                                      |
| changeStreamMode   | optional | Enum                | Specify either `struct` or `mutation` as the output type. The default is `struct`                                                                            |
| metadataInstance   | optional | String              | Specify spanner instance to use for the change streams connector metadata table. The default is same as `instanceId`                                         |
| metadataDatabase   | optional | String              | Specify spanner database to use for the change streams connector metadata table. The default is same as `databaseId`                                         |
| metadataTable      | optional | String              | Specify change stream connector metadata table name. If not provided, automatically created.                                                                 |
| inclusiveStartAt   | optional | String              | Specify timestamp to read change streams from. The starting DateTime inclusive                                                                               |
| inclusiveEndAt     | optional | String              | Specify timestamp to read change streams to. The ending DateTime inclusive                                                                                   |
| outputChangeRecord | optional | Boolean             | Specify output original change stream record. the default is true.                                                                                           |
| tables             | optional | Array<String\>      | Specify table names you want to output.                                                                                                                      |
| renameTables       | optional | Map<String,String\> | Specify a map of corresponding table names if you want to change the destination table name when inserting direct change record into another Spanner tables. |

#### changeStreamMode

| name     | description                                                                       |
|----------|-----------------------------------------------------------------------------------|
| struct   | Output changed record itself and additionally output as structs in table schemas. |
| mutation | Output change record as mutation to insert another spanner table.                 |

### Spanner source module parameters for microbatch mode

(Microbatch mode is in experimental)

| parameter             | optional | type    | description                                                                                                                                                                                       |
|-----------------------|----------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| intervalSecond        | optional | Integer | The interval at which the query is executed. Default is 60 seconds.                                                                                                                               |
| gapSecond             | optional | Integer | Buffer time to allow a margin between the end of the time range of the query and the current time. Default is 30 seconds.                                                                         |
| maxDurationMinute     | optional | Integer | Maximum time range for the query. This value is used if the interval between the start of the time range of the query and the current time is greater than intervalSecond. Default is 60 minutes. |
| catchupIntervalSecond | optional | Integer | The interval at which the query will be executed if the interval between the query start time and the current time is large. The unit is seconds. Default is the same as intervalSecond.          |
| startDatetime         | optional | String  | Start time of the first query. If not set, the value of outputCheckpoint will be used.                                                                                                            |
| outputCheckpoint      | optional | String  | Specify the GCS path if you want to record the latest time when the query was executed in GCS.                                                                                                    |

For more information about microbatch mode parameters, please refer to [microbatch page](microbatch.md).

## Related example config files

* [Cloud Spanner to BigQuery](../../../../examples/spanner-to-bigquery.json)
* [Cloud Spanner to Cloud Storage(Avro)](../../../../examples/spanner-to-avro.json)
* [Cloud Spanner to Cloud Datastore](../../../../examples/spanner-to-datastore.json)
* [Cloud Spanner to Cloud SQL](../../../../examples/spanner-to-jdbc.json)
* [Cloud Spanner to Cloud Spanner(Insert)](../../../../examples/spanner-to-spanner.json)
* [Cloud Spanner to Cloud Spanner(Delete)](../../../../examples/spanner-to-spanner-delete.json)
* [Cloud Spanner changestream to BigQuery](../../../../examples/spanner-changestream-to-bigquery.json)
* [Cloud Spanner changestream copy to Cloud Spanner](../../../../examples/spanner-changestream-to-spanner.json)
* [Cloud Spanner microbatch to BigQuery](../../../../examples/spanner-microbatch-to-bigquery.json)
