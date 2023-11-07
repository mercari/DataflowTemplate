# BigQuery Sink Module

Sink module to write the input data to a specified BigQuery table.

## Sink module common parameters

| parameter         | optional | type                     | description                                                                                                                                                                                              |
|-------------------|----------|--------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| name              | required | String                   | Step name. specified to be unique in config file.                                                                                                                                                        |
| module            | required | String                   | Specified `bigquery`                                                                                                                                                                                     |
| input (or inputs) | required | String or Array<String\> | Step name whose data you want to write from. If you want to write multiple inputs with the same schema to the same table, use the `inputs` parameter instead of `input` to specify multiple input names. |
| parameters        | required | Map<String,Object\>      | Specify the following individual parameters.                                                                                                                                                             |

## BigQuery sink module parameters

| parameter                 | optional | type           | description                                                                                                                                                                                                                                                                       |
|---------------------------|----------|----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| table                     | required | String         | Specify the Table to be written in BigQuery. {project}:{dataset}. {table} format                                                                                                                                                                                                  |
| writeDisposition          | optional | Enum           | One of `WRITE_TRUNCATE`, `WRITE_APPEND`, or `WRITE_EMPTY`. The default is `WRITE_EMPTY`                                                                                                                                                                                           |
| createDisposition         | optional | Enum           | One of `CREATE_IF_NEEDED` and `CREATE_NEVER`. The default is `CREATE_NEVER`.                                                                                                                                                                                                      |
| method                    | optional | Enum           | One of `FILE_LOADS`, `STREAMING_INSERTS`, `STORAGE_WRITE_API`, or `STORAGE_API_AT_LEAST_ONCE`. If it is not specified, it is determined automatically.                                                                                                                            |
| dynamicDestination        | optional | String         | Specify if you want to save each record to a different table. Specify a field name with the table name as a value.                                                                                                                                                                |
| partitioning              | optional | Enum           | Specifies that you want to save the data in the partition table when the destination table is generated automatically. One of `DAY` or `HOUR` is specified. The default is disabled. (This parameter is effective only when the table is first auto-generated)                    |
| partitioningField         | optional | String         | Specify the field name you want to specify as the destination partition when saving to Partition Table. (This parameter is effective only when the table is first auto-generated)                                                                                                 |
| clusteringFields          | optional | Array<String\> | Specify field names to split cluster for Clustering. (This parameter is effective only when the table is first auto-generated. clustering parameter is deprecated)                                                                                                                |
| primaryKeyFields          | optional | Array<String\> | Specify field names to primary key. (This parameter is effective only when the table is first auto-generated)                                                                                                                                                                     |
| skipInvalidRows           | optional | Boolean        | Insert all valid rows of a request, even if invalid rows exist. Default is false. (this option only for streaming mode)                                                                                                                                                           |
| ignoreUnknownValues       | optional | Boolean        | Accept rows that contain values that do not match the schema. Default is false.                                                                                                                                                                                                   |
| ignoreInsertIds           | optional | Boolean        | Setting this option to true disables insertId based data [deduplication offered by BigQuery](https://cloud.google.com/bigquery/streaming-data-into-bigquery#disabling_best_effort_de-duplication). Default is false. (this option only for streaming mode)                        |
| withExtendedErrorInfo     | optional | Boolean        | Enables extended error information. Default is false. (this option only for streaming mode)                                                                                                                                                                                       |
| failedInsertRetryPolicy   | optional | Enum           | Specfies a policy for handling failed inserts. You can specify one of the values `always`,`never`, or `retryTransientErrors`. Default is `always` This is only applicable when the write method is `STREAMING_INSERTS` and only streaming mode.                                   |
| kmsKey                    | optional | String         | kmsKey                                                                                                                                                                                                                                                                            |
| schemaUpdateOptions       | optional | Array<Enum\>   | Allows the schema of the destination table to be updated as a side effect of the write. Current support `ALLOW_FIELD_ADDITION` and `ALLOW_FIELD_RELAXATION`. Only applicable when method is `FILE_LOADS`.                                                                         |
| optimizedWrites           | optional | Boolean        | If true, enables new codepaths that are expected to use less resources while writing to BigQuery. Not enabled by default in order to maintain backwards compatibility.                                                                                                            |
| autoSharding              | optional | Boolean        | If true, enables using a dynamically determined number of shards to write to BigQuery. This can be used for both `FILE_LOADS` and `STREAMING_INSERTS`. Only applicable to streaming mode. The default is false.                                                                   |
| triggeringFrequencySecond | optional | Integer        | Specify the frequency second at which file writes are triggered. The default is 10. This is only applicable when the write method is `FILE_LOADS`, `STORAGE_WRITE_API` or `STORAGE_API_AT_LEAST_ONCE` and only streaming mode.                                                    |
| writeFormat               | optional | Enum           | The format for writing to BigQuery is automatically determined. Use this option when you want to specify the format directly, for example, for performance issues or to support schemas that are not supported by the various formats. The options are `json`, `avro`, and `row`. |

## BigQuery Write Method

| method                    | description                                                  |
|---------------------------|--------------------------------------------------------------|
| FILE_LOADS                | Use BigQuery load jobs to insert data.                       |
| STREAMING_INSERTS         | Use the BigQuery streaming insert API to insert data.        |
| STORAGE_WRITE_API         | Use the new, exactly-once Storage Write API.                 |
| STORAGE_API_AT_LEAST_ONCE | Use the new, Storage Write API without exactly once enabled. |

## Related example config files

* [Cloud Spanner to BigQuery](../../../../examples/spanner-to-bigquery.json)
* [Cloud SQL to BigQuery](../../../../examples/jdbc-to-bigquery.json)
* [Cloud PubSub(Avro) to BigQuery](../../../../examples/pubsub-avro-to-bigquery.json)
