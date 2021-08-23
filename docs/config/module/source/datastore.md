# Cloud Datastore Source Module

Source Module for loading data by specifying a gql into Cloud Datastore.

## Source module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `datastore` |
| schema | required | [Schema](SCHEMA.md) | Schema of the data to be read. (If you specify a `kind`, you don't need to specify it.) |
| parameters | required | Map<String,Object\> | Specify the following individual parameters |

## Datastore source module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| projectId | required | String | The GCP Project ID of the Datastore you want to load |
| gql | required | String | Query in GQL format to read data from Datastore. See [doc](https://cloud.google.com/datastore/docs/reference/gql_reference) for the formatting. |
| namespace | optional | String | Specify the namespace if you want to load |
| kind | optional | String | When you specify the kind name you want to read, it will estimate the kind's schema on a best-effort basis using [Datastore Stats](https://cloud.google.com/datastore/docs/concepts/stats). |
| withKey | optional | Boolean | Specify true if you want to read the Entity's Key as a `__key__` field. (Note that because `__key__` is a nested field, it cannot be used in sink module that do not support nested types.) |

## Related example config files

* [Cloud Datastore to Cloud Storage(Avro)](../../../../examples/datastore-to-avro.json)
* [Cloud Datastore GQL results to Delete](../../../../examples/datastore-to-delete.json)
