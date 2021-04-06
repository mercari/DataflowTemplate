# BigQuery Sink Module

Sink module to write the input data to a specified BigQuery table.

## Sink module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `bigquery` |
| input | required | String | Step name whose data you want to write from |
| parameters | required | Map<String,Object\> | Specify the following individual parameters. |

## BigQuery sink module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| table | required | String | Specify the Table to be written in BigQuery. {project}:{dataset}. {table} format |
| writeDisposition | optional | String | One of `WRITE_TRUNCATE`, `WRITE_APPEND`, or `WRITE_EMPTY`. The default is `WRITE_EMPTY` |
| createDisposition | optional | String | One of `CREATE_IF_NEEDED` and `CREATE_NEVER`. The default is `CREATE_NEVER`.|
| method | optional | String | One of `FILE_LOADS` and `STREAMING_INSERTS`. If it is not specified, it is determined automatically.|
| dynamicDestination | optional | String | Specify if you want to save each record to a different table. Specify a field name with the table name as a value. |
| partitioning | optional | String | Specifies that you want to save the data in the partition table when the destination table is generated automatically. One of `DAY` or `HOUR` is specified. The default is disabled.|
| partitioningField | optional | String | Specify the field name you want to specify as the destination partition when saving to Partition Table. |
| clustering | optional | String | Specify a split field name for Clustering. |
| skipInvalidRows | optional | Boolean | Insert all valid rows of a request, even if invalid rows exist. Default is false. (this option only for streaming mode) |
| ignoreUnknownValues | optional | Boolean | Accept rows that contain values that do not match the schema. Default is false. |
| ignoreInsertIds | optional | Boolean | Setting this option to true disables insertId based data [deduplication offered by BigQuery](https://cloud.google.com/bigquery/streaming-data-into-bigquery#disabling_best_effort_de-duplication). Default is false. (this option only for streaming mode) |
| withExtendedErrorInfo | optional | Boolean | Enables extended error information. Default is false. (this option only for streaming mode) |
| failedInsertRetryPolicy | optional | Enum | Specfies a policy for handling failed inserts. You can specify one of the values `always`,`never`, or `retryTransientErrors`. Default is `retryTransientErrors` which indicates that retry all failures except for known persistent errors. (this option only for streaming mode) |
| kmsKey | optional | String | kmsKey |

## Related example config files

* [Cloud Spanner to BigQuery](../../../../examples/spanner-to-bigquery.json)
* [Cloud SQL to BigQuery](../../../../examples/jdbc-to-bigquery.json)
* [Cloud PubSub(Avro) to BigQuery](../../../../examples/pubsub-avro-to-bigquery.json)
