# Cloud PubSub Source Module

PubSub source module for receiving message from specified Cloud PubSub topic or subscription.

## Source module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `pubsub` |
| schema | required | [Schema](SCHEMA.md) | Schema of the data to be read. (If you specify a `message` in the `format`, you don't need to specify it) |
| timestampAttribute | String | optional | If you want to use the value of an attribute of a PubSub message as the event time, specify the name of the attribute. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters |

## PubSub source module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| topic | selective required | String | Specify the topic to read data from PubSub; unnecessary if subscription is specified |
| subscription | selective required | String | Specify the subscription to read data from PubSub; unnecessary if topic is specified |
| format | required | String | Specify the format. Currently support `avro`, `json`, `protobuf` or `message` |
| idAttribute | optional | String | Specify the Attribute name you want to identify as id. [ref](https://cloud.google.com/dataflow/docs/concepts/streaming-with-cloud-pubsub#efficient_deduplication) |
| messageName | optional | String | When `protobuf` is specified as the `format`, Specify the full name(contains package name) of the target message. |
| validateUnnecessaryJsonField | optional | Boolean | When `json` is specified as the `format`, Specify true if you want to raise an error when a field that does not exist in the schema is found in the JSON. The default is false. |

※ If `protobuf` is specified in the `format`, both `protobufDescriptor` at [Schema](SCHEMA.md) and `messageName` at parameters must be specified.

※ If `message` is specified in the `format`, PubSub messages will be mapped to the following schema.

| field | type |
| --- | --- |
| payload | Bytes |
| messageId | String |
| attributes | Map<String,String\> |
| timestamp | Timestamp |


## Related example config files

* [Cloud PubSub(Avro) to BigQuery](../../../../examples/pubsub-avro-to-bigquery.json)
* [Cloud PubSub(Avro) to Cloud Spanner](../../../../examples/pubsub-avro-to-spanner.json)
* [Cloud PubSub(Json) to BeamSQL to Cloud PubSub(Json)](../../../../examples/pubsub-to-beamsql-to-pubsub.json)
* [Cloud PubSub(protobuf) to BeamSQL to Cloud PubSub(protobuf)](../../../../examples/pubsub-protobuf-to-beamsql-to-pubsub-protobuf.json)
