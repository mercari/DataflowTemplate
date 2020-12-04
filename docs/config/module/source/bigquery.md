# BigQuery Source Module

Source Module for loading data by specifying a query or table into BigQuery.

## Source module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `bigquery` |
| schema | - | [Schema](SCHEMA.md) | Schema of the data to be read. bigquery module does not require specification |
| parameters | required | Map<String,Object\> | Specify the following individual parameters. |

## BigQuery source module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| query | selective required | String | Specify the SQL to read data from BigQuery; unnecessary if table is specified |
| table | selective required | String | Specify a Table to load data from BigQuery ({project}. {dataset}. {table} format). If query is specified, it is not necessary. (You can't specify it in the case of view.) |
| queryTempDataset | optional | String | Optional when specifying a query. Specify a temporary Dataset to store the query results in. If not specified, a temporary Dataset will be created. |
| queryLocation | optional | String | Optional when specifying a query. Query execution location(ex: US) specification. |
| fields | optional | Array<String\> | Optional when specifying table. Specified when you want to narrow down the fields you want to read from the table. |
| rowRestriction | optional | String | Optional when specifying table. Specifies the conditions for refining the records of the table to be read. |

## Related example config files

* [BigQuery to Cloud Spanner](../../../../examples/bigquery-to-spanner.json)
* [BigQuery to Cloud Storage(Parquet)](../../../../examples/bigquery-to-parquet.json)
* [BigQuery to Cloud Datastore](../../../../examples/bigquery-to-datastore.json)
* [BigQuery to Cloud SQL](../../../../examples/bigquery-to-jdbc.json)
* [BigQuery to AWS S3(Avro)](../../../../examples/bigquery-to-aws-avro.json)
