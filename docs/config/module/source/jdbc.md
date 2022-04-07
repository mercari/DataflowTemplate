# JDBC Source Module

Source module for loading records from RDB using JDBC driver.
(Currently only support mysql, postgresql)

## Source module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `jdbc` |
| schema | - | [Schema](SCHEMA.md) | Schema of the data to be read. jdbc module does not require specification |
| parameters | required | Map<String,Object\> | Specify the following individual parameters |

## JDBC source module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| url | required | String | Connection destination for reading data in JDBC. |
| driver | required | String | Specify driver class such as `com.mysql.cj.jdbc.Driver`, `org.postgresql.Driver` |
| user | required | String | User name to access the database. You can also specify a Secret Manager resource name like `projects/{myproj}/secrets/{mysecret}/versions/latest` |
| password | required | String | User password to access the database. You can also specify a Secret Manager resource name like `projects/{myproj}/secrets/{mysecret}/versions/latest` |
| query | selective required | String | SQL for reading data in JDBC |
| table | selective required | String | Table name for reading data in JDBC. Used to retrieve all records of a table. Reduce the load by dividing the query into smaller pieces. It also dynamically divides the query range and executes it in a distributed manner for faster data retrieval. |
| prepareCalls | optional | Array<String\> | You can specify multiple prepareCalls for variable definitions, etc. when calling stored procedures. |
| prepareParameterQueries | optional | Array<PrepareQuery\> | When a parameterized query is defined in a `query` parameter, you can define a query that generates parameters and inserts the results of the execution into the `query`. The field values in the query result will be inserted in parameter order. |
| fields | optional | String | When a `table` parameter is specified, specify the fields to be retrieved. The text specified here will be inserted into the SELECT clause. |
| keyFields | conditional required | Array<String\> | When a `table` parameter is specified, specify the filter field for repeated data retrieval. Normally, specify the primary keys of the table |
| fetchSize | optional | Integer | When a `table` parameter is specified, specify the number of records to be retrieved at one time. |
| enableSplit | optional | Boolean | When a `table` parameter is specified, specify true to dynamically divide the query range according to the load and distribute the query execution. The default is false. |
| splitSize | optional | Integer | When a `table` parameter is specified and `enableSplit` is true, specify the number of dynamic splits for the query range. The default is 10. |
| excludeFields | optional | Array<String\> | Specify the field names if you want to exclude fields from the retrieved records. |

## PrepareQuery parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| query | required | String | Specify the query to retrieve variables to be inserted into the parameterized query |
| prepareCalls | optional | Array<String\> | Specify prepareCalls for query |

## Related example config files

* [Cloud SQL to BigQuery](../../../../examples/jdbc-to-bigquery.json)
* [Cloud SQL to Spanner](../../../../examples/jdbc-to-spanner.json)
* [Cloud SQL to BigQuery(Using prepareParameterQueries)](../../../../examples/jdbc-to-bigquery-with-prep-param-queries.json)
* [Cloud SQL Table to Spanner](../../../../examples/jdbc-table-to-spanner.json)
