# Datastore Sink Module

Sink module to write the input data to a specified Cloud Datastore kind.

## Sink module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `datastore` |
| input | required | String | Step name whose data you want to write from |
| parameters | required | Map<String,Object\> | Specify the following individual parameters. |

## Datastore sink module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| projectId | required | String | GCP ProjectID with a Datastore to write to |
| kind | required | String | Kind name to save |
| keyFields | optional | Array<String\> | Field name as a unique key when saving. Multiple fields may be specified. If not specified, it is automatically assigned by UUID. |
| delete | optional | Boolean | true if you want to delete it instead of writing it. If true is specified, keyFields must be specified. |

## Related example config files

* [BigQuery to Cloud Datastore](../../../../examples/bigquery-to-datastore.json)
* [Cloud Spanner to Cloud Datastore](../../../../examples/spanner-to-datastore.json)
* [Cloud Storage(Avro) to Cloud Datastore](../../../../examples/avro-to-datastore.json)
