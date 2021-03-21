# BeamSQL Transform Module

Transform Modules for processing and combining input data with a given SQL.

## Transform module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `beamsql` |
| inputs | required | Array<String\> | Specify the names of the step from which you want to process the data, including the name of the transform. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters. |

## BeamSQL transform module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| sql | required |  String | sql text to process inputs data. You can also specify the path of the GCS where you put the SQL file. |

## Related example config files

* [BeamSQL: Join BigQuery and Spanner table](../../../../examples/beamsql-join-bigquery-and-spanner-to-spanner.json)
* [Cloud PubSub(Json) to BeamSQL to Cloud PubSub(Json)](../../../../examples/pubsub-to-beamsql-to-pubsub.json)
