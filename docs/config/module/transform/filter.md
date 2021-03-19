# Filter Transform Module

Filter transform module can be used to filter rows by specified criteria or to retrieve only specified columns.

## Transform module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `partition` |
| inputs | required | Array<String\> | Specify the names of the step from which you want to process the data, including the name of the transform. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters. |

## Filter transform module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| filters | selective required | [FilterCondition](filtercondition.md) | Specify the conditions for filtering rows. |
| fields | selective required | Array<String\> | Specify a list of field names to be passed through. You can also specify nested fields by joining them with dots. |

* It is not possible to not specify both `filters` and `fields`


## Related example config files

* [Split Avro records to Cloud Spanner](../../../../examples/avro-to-filter-to-avro.json)
