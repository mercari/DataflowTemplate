# Storage Sink Module

Sink module to write the input data to a specified file storage path.

## Sink module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `storage` |
| input | required | String | Step name whose data you want to write from |
| parameters | required | Map<String,Object\> | Specify the following individual parameters. |

## Storage sink module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| output | required |  String | GCS or S3 path for file writing destination. |
| format | required | String | One of `avro`, `csv`, `json`, or `parquet`. |
| numShards | optional | Integer | he number of divisions of the file to be written out. |
| prefix | optional | String | File name prefix.|
| suffix | optional | String | File name suffix. |
| compression | optional | String | Select the compression format of the file. One of `ZIP`, `GZIP`, `BZIP2`, `ZSTD`, `UNCOMPRESSED`, or `AUTO`. The default is `AUTO`.|
| dynamicSplitField | optional | String | If you specify a field name, for each value, the records will be output in a separate path. If specified, its value will be the output terminal Prefix. |
| withoutSharding | optional | Boolean | Specifies whether to combine the output files into one. Default is false. |
| tempDirectory | optional | String | The GCS path of the temporary file export destination. If not specified, the bucket creation permission is required. |
| outputNotify | optional | String | Specify the GCS path if you want to also write out a list of destination file paths after the writing is finished. Even if the number of writes is zero, an empty file will be created. |
| outputEmpty | optional | Boolean | Specifies whether to output an empty file even if there are no write records. Default is false. |
| datetimeFormat | optional | String | Format of the time in the name of the export file if Window is specified. (Specify yyyyMMdd, etc.) |
| datetimeFormatZone | optional | String | TimeZone of the time in the name of the export file if you specify a Window (default is `Etc/GMT`). |
| useOnlyEndDatetime | optional | Boolean | If you want to use only the closing time of the window in the time of the export file name with the Window specified, specify it. (The default is `false`.)|

## Related example config files

* [BigQuery to Cloud Storage(Parquet)](../../../../examples/bigquery-to-parquet.json)
* [BigQuery to AWS S3(Avro)](../../../../examples/bigquery-to-aws-avro.json)
* [Cloud Spanner to Cloud Storage(Avro)](../../../../examples/spanner-to-avro.json)
* [Cloud Datastore to Cloud Storage(Avro)](../../../../examples/datastore-to-avro.json)
