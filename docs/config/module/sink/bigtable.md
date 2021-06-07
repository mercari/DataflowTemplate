# Bigtable Sink Module (Experimental)

Sink module to write the input data to a specified Cloud Bigtable table.

## Sink module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `bigtable` |
| input | required | String | Step name whose data you want to write from |
| parameters | required | Map<String,Object\> | Specify the following individual parameters. |

## BigQuery sink module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| projectId | required | String | Cloud Bigtable's GCP Project ID that you want to write  |
| instanceId | required | String | The Instance ID of the Cloud Bigtable you want to write |
| tableId | required | String | The table name of the Cloud Bigtable you want to write |
| columnFamily | required | String | Specify columnFamily name. If you want to specify each field separately, use `columnSettings` as described below |
| rowKeyFields | selective required | Array<String\> | Specify the field you want to use as the rowKey value. The values of the fields will be converted to strings and concatenated with # in the order specified here |
| rowKeyTemplate | selective required | String | Specify the template text when you want to specify the rowKey value by conversion using template engine [FreeMarker](https://freemarker.apache.org/) |
| format | optional | String | Specify the serialization format.　One of `bytes`, `string` or `avro`. The default is `string`. If you choose `avro`, then the entire record will be serialized and saved in Avro format as a single field, not field by field |
| columnSettings | optional | Array<ColumnSetting\> | Specify the settings for each column. If you don't specify anything here, the values specified in `format` and `columnFamily` will be applied to all fields in the record, and the field name will be columnQualifier as it is |
| columnQualifier | optional | String | Specify the columnQualifier to be saved when `avro` is selected for `format`. The default is `body` |

## ColumnSetting parameters

Specify the settings for each column.

| parameter | optional | type | description |
| --- | --- | --- | --- |
| field | required | String | Specify the name of the field to be configured |
| format | optional | String | Specify the serialization format.　One of `bytes`, `string` or `avro` |
| columnFamily | optional | String | Specify the columnFamily to be assigned to the field |
| columnQualifier | optional | String | Specify the columnQualifier to be assigned to the field |
| exclude | optional | Boolean | Specify if you want to exclude the field from storing. The default is false |


### Signatures of build-in utility functions for template engine

There are built-in functions for date and timestamp formatting available in the `rowKeyTemplate`.

```
// Text format function for date field
${_DateTimeUtil.formatDate(dateField, 'yyyyMMdd')}

// Text format function for timestamp field
${_DateTimeUtil.formatTimestamp(timestampField, 'yyyyMMddhhmmss', 'Asia/Tokyo')}

// The event timestamp implicitly assigned to a record can be referenced by _EVENTTIME.
${_DateTimeUtil.formatTimestamp(_EVENTTIME, 'yyyyMMddhhmmss', 'Asia/Tokyo')}
```

## Related example config files

* [BigQuery to Cloud Bigtable](../../../../examples/bigquery-to-bigtable.json)
