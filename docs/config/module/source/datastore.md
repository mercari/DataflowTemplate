# Cloud Datastore Source Module

Source Module for loading data by specifying a gql into Cloud Datastore.

## Source module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `datastore` |
| schema | required | [Schema](SCHEMA.md) | Schema of the data to be read |
| parameters | required | Map<String,Object\> | Specify the following individual parameters |

## BigQuery source module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| projectId | required | String | The GCP Project ID of the Datastore you want to load |
| gql | required | String | Query in GQL format to read data from Datastore. See [doc](https://cloud.google.com/datastore/docs/reference/gql_reference) for the formatting. |
| namespace | optional | String | Specify the namespace if you want to load |

## Related example config files

* [Cloud Datastore to Cloud Storage(Avro)](../../../../examples/datastore-to-avro.json)
