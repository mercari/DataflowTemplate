# BeamSQL Transform Module

Transform Modules for processing and combining input data with a given SQL.

Refer to [Beam's official documentation](https://beam.apache.org/documentation/dsls/sql/overview/) for Beam SQL specifications.
In additional to that, `beamsql` module provides a variety of original [built-in UDFs](beamsqludf.md).

If you want to set a trigger for the stream execution, use the [window](window.md) module for the input data.

Note that `zetasql` planner does not support INT32 and FLOAT32 data types as input in `beamsql` module.

## Transform module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specify `beamsql` |
| inputs | required | Array<String\> | Specify the names of the step from which you want to process the data, including the name of the transform. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters. |

## BeamSQL transform module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| sql | required | String | sql text to process inputs data. You can also specify the path of the GCS where you put the SQL file. |
| planner | optional | Enum | Specify beamsql query planner name, [`zetasql`](https://beam.apache.org/documentation/dsls/sql/zetasql/overview/) or [`calcite`](https://beam.apache.org/documentation/dsls/sql/calcite/overview/). By default, the value of the [config.settings.beamsql.plannerName](../../README.md) option will be applied. |

## Related example config files

* [BeamSQL: Join BigQuery and Spanner table](../../../../examples/beamsql-join-bigquery-and-spanner-to-spanner.json)
* [Cloud PubSub(Json) to BeamSQL to Cloud PubSub(Json)](../../../../examples/pubsub-to-beamsql-to-pubsub.json)
