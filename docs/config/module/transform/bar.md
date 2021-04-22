# Bar Transform Module

Bar transform module generates financial bars from price movement records.

## Transform module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `bar` |
| inputs | required | Array<String\> | Specify the names of the step from which you want to process the data, including the name of the transform. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters. |

## Bar transform module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| priceField | required | String | Specify the field name with the price value of the record. |
| timestampField | required | String | Specify the field name with the timestamp value of the record |
| volumeField | optional | String | Specify the field name with the value of the transaction volume. |
| symbolFields | optional | Array<String\> | When the data to be handled contains multiple symbols, specify the field names with the value of the issue ID of the record. |
| type | optional | Enum | Specify the bar type. Currently, only `time` is supported. |
| unit | optional | Enum | Specify the time magnitude of the bar. `second`,`minute`,`hour`,`day`,`week`, and `month` are supported. |
| size | optional | Integer | Specify the size of the bar. |
| timezone | optional | String | Specify the [timezone](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones). Default is `Etc/GMT`. |
| isVolumeAccumulative | optional | Boolean | Specify whether the value of volume is accumulative rather than per transaction. Default is false. |


## Output Schema

| field | type | description |
| --- | --- | --- |
| symbol | String | The value of symbol, or if multiple fields are specified in `symbolFields`, a string with their values concatenated with `#`. |
| open | Float | Opening price of the bar. |
| close | Float | Closing price of the bar. |
| low | Float | Lowest price of the bar. |
| high | Float | Highest price of the bar. |
| volume | Float | Transaction volume of the bar. |
| count | Integer | Number of records processed for the bar. |
| vwap | Float | [Trading volume weighted average price](https://en.wikipedia.org/wiki/Volume-weighted_average_price). |
| sizeSecond | Integer | The time difference between the oldest and newest records in the bar. |
| timestamp | Timestamp | End time of the bar. |

* When `isVolumeAccumulative` is true, `vwap` is calculated based on count, not volume.

## Related example config files

* [Cloud PubSub(Json) to Bar to BigQuery](../../../../examples/pubsub-to-bar-to-bigquery.json)
