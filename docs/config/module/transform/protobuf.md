# Protobuf Transform Module

Protobuf transform module deserialize byte array serialized in ProtocolBuffer format for the specified fields.

## Transform module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `protobuf` |
| inputs | required | Array<String\> | Specify the names of the step from which you want to process the data, including the name of the transform. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters. |

## Protobuf transform module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| descriptorFilePath | required | String | Specify the path of the GCS where the descriptor file containing the Message to be deserialized is located. |
| fields | required | Array<ProtoField\> | Specify the field of the input record that you want to deserialize. Multiple fields can be specified. |
| failFast | optional | Boolean | Specify whether the job should fail immediately if there are records that have failed to deserialize. Default is True. |

## ProtoField parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| field | required | String | Field name of the input record to be deserialized. |
| messageName | required | String | Message name of the ProtocolBuffer corresponding to the deserialization. Specify the full name including the package name. |
| outputField | optional | String | Specify the output field name when you want to output the deserialization result as a separate output field without overwriting the input field. |


## Related example config files

* [Spanner Protobuf deserialize to Cloud Storage(Avro)](../../../../examples/spanner-to-protobuf-to-avro.json)
