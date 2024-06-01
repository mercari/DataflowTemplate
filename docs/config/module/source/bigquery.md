# BigQuery Source Module

Source Module for loading data by specifying a query or table into BigQuery.

## Source module common parameters

| parameter          | optional | type                | description                                                                                                                                                                   |
|--------------------|----------|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| name               | required | String              | Step name. specified to be unique in config file.                                                                                                                             |
| module             | required | String              | Specified `bigquery`                                                                                                                                                          |
| schema             | -        | [Schema](SCHEMA.md) | Schema of the data to be read. bigquery module does not require specification                                                                                                 |
| microbatch         | optional | Boolean             | Specify true if you want to retrieve data in near real time using the Micorobatch method. Default is false. (You need to start Dataflow in streaming mode if microbatch mode) |
| timestampAttribute | optional | String              | If you want to use the value of an field as the event time, specify the name of the field. (The field must be Timestamp or Date type)                                         |
| parameters         | required | Map<String,Object\> | Specify the following individual parameters.                                                                                                                                  |

## BigQuery source module parameters

| parameter        | optional           | type           | description                                                                                                                                                                                                                                                                                  |
|------------------|--------------------|----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| query            | selective required | String         | Specify the SQL to read data from BigQuery. unnecessary if table is specified. You can also specify the path of the GCS where you put the SQL file.                                                                                                                                          |
| table            | selective required | String         | Specify a Table to load data from BigQuery ({project}. {dataset}. {table} format). If query is specified, it is not necessary. (You can't specify it in the case of view.)                                                                                                                   |
| queryTempDataset | optional           | String         | Optional when specifying `query`. Specify a temporary Dataset to store the query results in. If not specified, a temporary Dataset will be created. Note that if this is not specified, additional permission to create and delete dataset will be required.                                 |
| queryLocation    | optional           | String         | Optional when specifying `query`. Query execution location(ex: US) specification. If not specified, the execution location is automatically estimated from the dataset included in the query. Note that if this option is not specified, additional dataset.get permission will be required. |
| fields           | optional           | Array<String\> | Optional when specifying `table`. Specified when you want to narrow down the fields you want to read from the table.                                                                                                                                                                         |
| rowRestriction   | optional           | String         | Optional when specifying `table`. Specifies the conditions for refining the records of the table to be read.                                                                                                                                                                                 |

### BigQuery source module parameters for microbatch mode

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

* [BigQuery to Cloud Spanner](../../../../examples/bigquery-to-spanner.json)
* [BigQuery to Cloud Storage(Parquet)](../../../../examples/bigquery-to-parquet.json)
* [BigQuery to Cloud Datastore](../../../../examples/bigquery-to-datastore.json)
* [BigQuery to Cloud SQL](../../../../examples/bigquery-to-jdbc.json)
* [BigQuery to AWS S3(Avro)](../../../../examples/bigquery-to-aws-avro.json)
* [BigQuery microbatch to Cloud Spanner](../../../../examples/bigquery-microbatch-to-spanner.json)
