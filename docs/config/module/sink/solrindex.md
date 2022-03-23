# Solr index Sink Module (Experimental)

Sink module to write solr index file to a specified file storage path.

## Sink module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `solrindex` |
| input | required | String | Step name whose data you want to write from |
| sideInputs | optional | Array<String\> | Specify input names to be used for custom dictionaries or blackwords |
| parameters | required | Map<String,Object\> | Specify the following individual parameters. |

## Solr index sink module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| output | required |  String | GCS path for file writing destination. |
| coreName | required |  String | Solr core name to create index. |
| indexSchema | optional |  String | GCS path for solr index schema file. |
| confDirectoryOutput | optional |  String | GCS path for the schema file, dictionary, and other configuration files |
| dynamicSplitField | optional | String | If you specify a field name, for each value, the records will be output in a separate path. If specified, its value will be the output terminal Prefix. |
| tempDirectory | optional | String | The GCS path of the temporary file export destination. If not specified, the bucket creation permission is required. |
| datetimeFormat | optional | String | Format of the time in the name of the export file if Window is specified. (Specify yyyyMMdd, etc.) |
| datetimeFormatZone | optional | String | TimeZone of the time in the name of the export file if you specify a Window (default is `Etc/GMT`). |
| useOnlyEndDatetime | optional | Boolean | If you want to use only the closing time of the window in the time of the export file name with the Window specified, specify it. (The default is `false`.)|


### FileSetting parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| inputName | selective required | String | Specify the name of the input that will generate the file |
| inputPath | selective required | String | Specify the GCS path of the file |
| filename | required | String | Specify the name of the file to be referenced during the indexing. |
| field | selective required | String | To generate a file from input, specify the field of input. The value of this field will be written as a single line. |


## Related example config files

* [BigQuery to Solr Index(PDF parse)](../../../../examples/bigquery-pdf-to-solrindex.json)
