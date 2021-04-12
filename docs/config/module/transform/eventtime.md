# Eventtime Transform Module

Eventtime transform module extracts the event time implicitly assigned to a record as the value of a specified field, or sets the time value of a specified field as the event time of the record.

## Transform module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `eventtime` |
| inputs | required | Array<String\> | Specify the names of the step from which you want to process the data, including the name of the transform. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters. |

## Eventtime transform module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| eventtimeField | required | String | Specify the field name to which you want to give the event time of the record (when `into` is set to true), or the field name with the value you want to set as the event time of the record (when `into` is set to false). |
| into | optional | Boolean | Specify whether to extract the event time of the record as the value of the field (set to true) or to set the value of the field as the event time of the record (set to false). Default is true. |
| allowedTimestampSkewSeconds | optional | Integer | When the value of a field is given as the event time of a record (specifying false for `into`), an error will occur if the time is set later than the [watermark](https://beam.apache.org/documentation/programming-guide/#watermarks-and-late-data). To avoid this error, specify the seconds that are allowed to be delayed from the watermark. |

* When `into` is set to true, the field specified in `eventtimeField` will be automatically added to the output record as a timestamp type.
* When `into` is set to false, the field specified in `eventtimeField` must be defined as a non-null timestamp type in the input record.

## Related example config files

* [Cloud PubSub(JSON) to BeamSQL to Cloud PubSub(JSON)](../../../../examples/pubsub-to-beamsql-to-pubsub.json)
