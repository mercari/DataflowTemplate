# Window Transform Module

Window module specifies the time frame and execution timing for aggregate processing of data.

Refer to [Beam's official documentation](https://beam.apache.org/documentation/programming-guide/#windowing) for window specifications.

## Transform module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specify `window` |
| inputs | required | Array<String\> | Specify the names of the step from which you want to process the data, including the name of the transform. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters. |

## Window transform module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| type | required | Enum | Window [type](https://beam.apache.org/documentation/programming-guide/#provided-windowing-functions). One of `global`, `fixed`, `sliding`, `session`, or `calendar`. |
| unit | optional | String | Window size unit. One of `second`, `minute`, `hour`, `day`, `week`, `month`, or `year`. The default is `second`. |
| size | selective required | Integer | Window size. required if type is `fixed` or `sliding` or `calendar` |
| frequency | selective required | Integer | Window trigger frequency. required if type is `sliding`. the unit is the same as specified in `unit`. |
| gap | selective required | Integer | Window trigger gap. required if type is `session`. |
| offset | optional | Integer | Window offset (for `fixed` or `sliding` window). the unit is the same as specified in `unit`. |
| trigger | optional | Trigger | Window [trigger](https://beam.apache.org/documentation/programming-guide/#setting-a-trigger) setting. |
| allowedLateness | optional | Integer | [allowedLateness size](https://beam.apache.org/documentation/programming-guide/#handling-late-data). the unit is the same as specified in `unit`. |
| timezone | optional |  String | timezone. |
| startingYear | optional | Integer | Specify if type is calendar and you want to specify a starting point. |
| startingMonth | optional | Integer | Specify if type is calendar and you want to specify a starting point. |
| startingDay | optional | Integer | Specify if type is calendar and you want to specify a starting point. |
| accumulationMode | optional | Enum | Trigger [accumulation mode](https://beam.apache.org/documentation/programming-guide/#window-accumulation-modes). One of `discarding` or `accumulating`. the default is `discarding`. |
| discardingFiredPanes | optional | Boolean | (Deprecated. use `accumulationMode`) Discarding fired panes or not. the default is true. |

## Trigger parameters

This setting specifies the [trigger for the window](https://beam.apache.org/documentation/programming-guide/#triggers).

| parameter | optional | type | description |
| --- | --- | --- | --- |
| type | required | Enum | Trigger [type](https://beam.apache.org/documentation/programming-guide/#triggers). One of `afterWatermark`, `afterProcessingTime`, `afterPane`, `repeatedly`, `afterEach`, `afterFirst`, or `afterAll`. |
| earlyFiringTrigger | optional | Trigger | (For `afterWatermark` trigger) Specify the trigger that you want to fire before the watermark. |
| lateFiringTrigger | optional | Trigger | (For `afterWatermark` trigger) Specify the trigger that you want to fire after the watermark. |
| childrenTriggers | selective required | Array<Trigger\> | (For [composite triggers](https://beam.apache.org/documentation/programming-guide/#composite-triggers) such as `afterEach`, `afterFirst`, `afterAll` trigger) Specify triggers that are the firing conditions for the composite trigger. |
| foreverTrigger | selective required | Trigger | (For `repeatedly` trigger) specifies a trigger that executes forever. |
| finalTrigger | optional | Trigger | Specify a trigger for final condition to cause any trigger to fire one final time and never fire again |
| pastFirstElementDelay | selective required | Integer | (For `afterProcessingTime` trigger) Specify the interval of time that has elapsed since the arrival of the first data. |
| pastFirstElementDelayUnit | selective required | Enum | (For `afterProcessingTime` trigger) Specify the unit of pastFirstElementDelay. One of `second`, `minute`, `hour`, or `day`. The default is `second`. |
| elementCountAtLeast | selective required | Integer | (For `afterPane` trigger) Specify the number of data to be the firing condition |


## Related example config files

* [BigQuery to Cloud Storage(Parquet)](../../../../examples/bigquery-to-parquet.json)
* [Cloud Datastore to Cloud Storage(Avro)](../../../../examples/datastore-to-avro.json)
