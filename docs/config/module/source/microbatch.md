# Source module microbatch mode

For micro-batch data retrieval, you can define a query that uses predefined variables such as `__EVENT_EPOCH_SECOND_PRE__` and `__EVENT_EPOCH_SECOND__` to retrieve data.

The following SQL for Spanner is an example of constructing a query using these variables.

```SQL
SELECT *
FROM
  MyTable
WHERE
  CreatedAt >= TIMESTAMP_SECONDS(${__EVENT_EPOCH_SECOND_PRE__})
  AND CreatedAt < TIMESTAMP_SECONDS(${__EVENT_EPOCH_SECOND__})
```

At the time of query execution, the variables `__EVENT_EPOCH_SECOND_PRE__` and `__EVENT_EPOCH_SECOND__` will be inserted with the specified interval of UNIX epoch seconds.

The following predefined variables can be used in SQL.

| variable | type | description |
| --- | --- | --- |
| __EVENT_EPOCH_SECOND__ | Integer | UNIX epoch seconds of the end time of the time range used as a filter in the query |
| __EVENT_EPOCH_SECOND_PRE__ | Integer | UNIX epoch seconds of the start time of the time range used as a filter in the query |
| __EVENT_EPOCH_MILLISECOND__ | Integer | UNIX epoch milli seconds of the end time of the time range used as a filter in the query |
| __EVENT_EPOCH_MILLISECOND_PRE__ | Integer | UNIX epoch milli seconds of the start time of the time range used as a filter in the query |
| __EVENT_DATETIME_ISO__ | String | string in ISO 8601 format of the end time of the time range to be used as a filter in the query |
| __EVENT_DATETIME_ISO_PRE__ | String | string in ISO 8601 format of the start time of the time range to be used as a filter in the query |

The frequency of query execution and the time range to be retrieved by the query can be controlled by the following parameters.

| parameter | optional | type | description |
| --- | --- | --- | --- |
| intervalSecond | optional | Integer | The interval at which the query is executed. Default is 60 seconds. |
| gapSecond | optional | Integer | Buffer time to allow a margin between the end of the time range of the query and the current time. Default is 30 seconds. |
| maxDurationMinute | optional | Integer | Maximum time range for the query. This value is used if the interval between the start of the time range of the query and the current time is greater than intervalSecond. Default is 60 minutes. |
| catchupIntervalSecond | optional | Integer | The interval at which the query will be executed if the interval between the query start time and the current time is large. The unit is seconds. Default is the same as intervalSecond. |
| startDatetime | optional | String | Start time of the first query. If not set, the value of outputCheckpoint will be used. |
| outputCheckpoint | optional | String | Specify the GCS path if you want to record the latest time when the query was executed in GCS. |


The following figure shows the relationship between these parameters.

<img src="https://raw.githubusercontent.com/mercari/DataflowTemplate/master/docs/images/microbath-parameters.png">


## Related example config files

* [Cloud Spanner Microbatch to BigQuery](../../../../examples/spanner-microbatch-to-bigquery.json)
* [BigQuery Microbatch to Cloud Spanner](../../../../examples/bigquery-microbatch-to-spanner.json)
