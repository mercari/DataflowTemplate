# Spanner Source Module

Source Module for loading data by specifying a query or table into Cloud Spanner.

## Source module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `spanner` |
| schema | - | [Schema](SCHEMA.md) | Schema of the data to be read. spanner module does not require specification |
| parameters | required | Map<String,Object\> | Specify the following individual parameters |

## Spanner source module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| projectId | required | String | The GCP Project ID of the Spanner you want to load |
| instanceId | required | String | The Instance ID of the Spanner you want to load |
| databaseId | required | String | The Database ID of the Spanner you want to load |
| query | selective required | String | Specify the SQL to read data from Spanner; not necessary if table is specified. |
| table | selective required | String | Specify the table name to read data from Spanner; not necessary if query is specified. |
| fields | optional | Array<String\> | Specify the name of the field you want to read from the table. The default is all fields. |
| timestampBound | optional | String | Specify when you want to read the data at the specified time. Format: `yyyy-MM-ddTHH:mm:SSZ` |

## Related example config files

* [Cloud Spanner to BigQuery](../../../../examples/spanner-to-bigquery.json)
* [Cloud Spanner to Cloud Storage(Avro)](../../../../examples/spanner-to-avro.json)
* [Cloud Spanner to Cloud Datastore](../../../../examples/spanner-to-datastore.json)
* [Cloud Spanner to Cloud SQL](../../../../examples/spanner-to-jdbc.json)
* [Cloud Spanner to Cloud Spanner(Insert)](../../../../examples/spanner-to-spanner.json)
* [Cloud Spanner to Cloud Spanner(Delete)](../../../../examples/spanner-to-spanner-delete.json)
