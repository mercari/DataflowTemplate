# Window Transform Module

Transform module for assigning input data to groups for collective processing on a time axis.

## Transform module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `window` |
| inputs | required | Array<String\> | Specify the names of the step from which you want to process the data, including the name of the transform. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters. |

## Window transform module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| type | required | String | Window type. One of `global`, `fixed`, `sliding`, `session`, or `calendar`. |
| unit | optional | String | Window size unit. One of `second`, `minute`, `hour`, `day`, `week`, `month`, or `year`. The default is `second`. |
| size | selective required | Integer | window size. required if type is `fixed` or `sliding` or `calendar` |
| frequency | selective required | Integer | window trigger frequency. required if type is `sliding`. |
| gap | selective required | Integer | window trigger gap. required if type is `session`. |
| offset | optional | Integer | window offset. |
| allowedLateness | optional | Integer | allowedLateness size. |
| timezone | optional |  String | timezone. |
| startingYear | optional | Integer | Specified if type is calendar and you want to specify a starting point. |
| startingMonth | optional | Integer | Specified if type is calendar and you want to specify a starting point. |
| startingDay | optional | Integer | Specified if type is calendar and you want to specify a starting point. |
| discardingFiredPanes | optional | Boolean | discardingFiredPanes. the default is true. |


## Related example config files

* [BigQuery to Cloud Storage(Parquet)](../../../../examples/bigquery-to-parquet.json)
* [Cloud Datastore to Cloud Storage(Avro)](../../../../examples/datastore-to-avro.json)
