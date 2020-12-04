# JDBC Source Module

Source Module for loading data by specifying a query into RDB using JDBC.
(Currently only support mysql)

## Source module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `jdbc` |
| schema | - | [Schema](SCHEMA.md) | Schema of the data to be read. jdbc module does not require specification |
| parameters | required | Map<String,Object\> | Specify the following individual parameters |

## BigQuery source module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| query | required | String | SQL for reading data in JDBC |
| url | required | String | Connection destination for reading data in JDBC. |
| driver | required | String | Specify driver class such as `com.mysql.cj.jdbc.Driver`, `org.postgresql.Driver` |
| user | required | String | User name to access the database |
| password | required | String | User password to access the database |

## Related example config files

* [Cloud SQL to BigQuery](../../../../examples/jdbc-to-bigquery.json)
