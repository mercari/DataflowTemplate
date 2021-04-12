# Cloud PubSub Source Module

PubSub source module for receiving message from specified Cloud PubSub topic or subscription.

## Source module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `pubsub` |
| schema | required | [Schema](SCHEMA.md) | Schema of the data to be read |
| timestampAttribute | String | optional | If you want to use the value of an attribute of a PubSub message as the event time, specify the name of the attribute. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters |

## PubSub source module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| topic | selective required | String | Specify the topic to read data from PubSub; unnecessary if subscription is specified |
| subscription | selective required | String | Specify the subscription to read data from PubSub; unnecessary if topic is specified |
| format | required | String | Specify the format. Currently support `avro` or `json` |
| idAttribute | optional | String | Specify the Attribute name you want to identify as id. [ref](https://cloud.google.com/dataflow/docs/concepts/streaming-with-cloud-pubsub#efficient_deduplication) |

## Related example config files

* [Cloud PubSub(Avro) to BigQuery](../../../../examples/pubsub-avro-to-bigquery.json)
* [Cloud PubSub(Avro) to Cloud Spanner](../../../../examples/pubsub-avro-to-spanner.json)
* [Cloud PubSub(Json) to BeamSQL to Cloud PubSub(Json)](../../../../examples/pubsub-to-beamsql-to-pubsub.json)