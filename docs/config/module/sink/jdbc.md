# JDBC Sink Module

Sink module to write the input data to a specified RDB table.

## Sink module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `jdbc` |
| input | required | String | Step name whose data you want to write from |
| parameters | required | Map<String,Object\> | Specify the following individual parameters. |

## JDBC sink module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| table | required | String | Destination table name. |
| url | required | String | Connection destination for reading data in JDBC. |
| driver | required | String | Specify driver class such as `com.mysql.cj.jdbc.Driver`, `org.postgresql.Driver` |
| user | required | String | User name to access the database |
| password | required | String | User password to access the database |
| createTable | optional | Boolean | Specify true if you want to generate the table automatically if the destination table does not exist. |
| keyFields | optional | Array<String\> | Required if true is specified in createTable. Specify the name of the fields you want to use as the primary key. |
| batchSize | optional | Integer | Specify the batch size when writing. |

## Related example config files

* [BigQuery to Cloud SQL](../../../../examples/bigquery-to-jdbc.json)
* [Cloud Spanner to Cloud SQL](../../../../examples/spanner-to-jdbc.json)
