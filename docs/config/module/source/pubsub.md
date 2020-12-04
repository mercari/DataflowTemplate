# Cloud PubSub Source Module

Source Module for loading data by specifying a gql into Cloud PubSub.

## Source module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `pubsub` |
| schema | required | [Schema](SCHEMA.md) | Schema of the data to be read |
| parameters | required | Map<String,Object\> | Specify the following individual parameters |

## BigQuery source module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| topic | selective required | String | Specify the topic to read data from PubSub; unnecessary if subscription is specified |
| subscription | selective required | String | Specify the subscription to read data from PubSub; unnecessary if topic is specified |
| format | required | String | Specify the format. Currently support `avro` only |
| idAttribute | optional | String | Specify the Attribute name you want to identify as id |

## Related example config files

* [Cloud PubSub(Avro) to BigQuery](../../../../examples/pubsub-avro-to-bigquery.json)
* [Cloud PubSub(Avro) to Cloud Spanner](../../../../examples/pubsub-avro-to-spanner.json)