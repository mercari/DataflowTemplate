# PubSub Sink Module

PubSub sink module publishes record to the specified PubSub topic.

## Sink module common parameters

| parameter        | optional | type                | description                                                                             |
|------------------|----------|---------------------|-----------------------------------------------------------------------------------------|
| name             | required | String              | Step name. specified to be unique in config file.                                       |
| module           | required | String              | Specified `pubsub`                                                                      |
| input            | required | String              | Step name whose data you want to write from                                             |
| parameters       | required | Map<String,Object\> | Specify the following individual parameters.                                            |
| outputAvroSchema | optional | String              | Save the schema of the output as an Avro Schema file in the path of GCS specified here. |

## PubSub sink module parameters

| parameter           | optional | type           | description                                                                                                                                                                                                                                                                         |
|---------------------|----------|----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic               | required | String         | Specify the PubSub topic name of the destination in the following format. `projects/{gcp project}/topics/{topic name}`                                                                                                                                                              |
| format              | required | String         | Specifies the format of the message to publish. Currently supporting `json`, `avro` and `protobuf`.                                                                                                                                                                                 |
| attributes          | optional | Array<String\> | Specify the field names you want to register as attributes in the message.                                                                                                                                                                                                          |
| idAttribute         | optional | String         | Specify the attribute name when you want to give an [ID](https://cloud.google.com/dataflow/docs/concepts/streaming-with-cloud-pubsub#efficient_deduplication) to a message as attribute value. The value of the record of the field with the name specified here is used as the ID. |
| timestampAttribute  | optional | String         | Specify the attribute name when you want to save the event time of the record as attribute value.                                                                                                                                                                                   |
| orderingKeyFields   | optional | Array<String\> | Specify the names of fields whose values you want to assign an [ordering key](https://cloud.google.com/pubsub/docs/ordering) to the message. If multiple field names are specified, their values are concatenated with `#`.                                                         |
| maxBatchSize        | optional | Integer        | Specify the number of buffers to send to PubSub at one time.                                                                                                                                                                                                                        |
| maxBatchBytesSize   | optional | Integer        | Specifies the buffer byte size to be sent to PubSub at one time.                                                                                                                                                                                                                    |
| protobufDescriptor  | optional | String         | When `protobuf` is specified as the `format`, specify the path of the GCS where the descriptor file for serialization is located.                                                                                                                                                   |
| protobufMessageName | optional | String         | When `protobuf` is specified as the `format`, Specify the full name(contains package name) of the target message.                                                                                                                                                                   |

* If you specify `orderingKeyFields`, `maxBatchSize` is automatically set to 1. This is because the current [PubSubIO does not support grouping by orderingKey](https://issues.apache.org/jira/browse/BEAM-13148).

## Related example config files

* [BigQuery to Cloud PubSub](../../../../examples/bigquery-to-pubsub.json)
* [Cloud PubSub(protobuf) to BeamSQL to Cloud PubSub(protobuf)](../../../../examples/pubsub-protobuf-to-beamsql-to-pubsub-protobuf.json)
* [Dummy to Cloud PubSub](../../../../examples/dummy-to-pubsub.json)
