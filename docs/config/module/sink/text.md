# Text Sink Module

Sink module to write the input data to a specified file storage path.

In contrast to StorageSink, which stores a lot of data at once, TextSink generates a file for each record and stores it in GCS.

TextSink uses FreeMarker, a popular template engine, to generate text freely according to the content of the data.

## Sink module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `storage` |
| input | required | String | Step name whose data you want to write from |
| parameters | required | Map<String,Object\> | Specify the following individual parameters. |

## Text sink module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| output | required |  String | GCS or S3 path for file writing destination. By defining the destination as a template in FreeMarker format, you can assemble the destination according to the data at runtime. |
| template | required | String | GCS path of the FreeMarker format Template file |
| contentType | optional | String | [Content-Type](https://tools.ietf.org/html/rfc7231#section-3.1.1.5). default is 'application/octet-stream' |
| bom | optional | Boolean | Set to true if the file is to be saved in BOM format. default is false |
| charset | optional | String | Charset. default is 'UTF-8' |
| cacheControl | optional | String | [Cache-Control](https://tools.ietf.org/html/rfc7234#section-5.2) |
| contentDisposition | optional | String | [Content-Disposition](https://tools.ietf.org/html/rfc6266) |
| contentEncoding | optional | String | [Content-Encoding](https://tools.ietf.org/html/rfc7231#section-3.1.2.2) |
| contentLanguage | optional | String | [Content-Language](https://tools.ietf.org/html/rfc7231#section-3.1.3.2) |
| metadata | optional | Map<String,String\> | User-provided metadata, in key/value pairs. |

## FreeMarker Build-in classes

The following convenience classes are provided for use with the Template Engine

| class | method | description |
| --- | --- | --- |
| _CSVPrinter | line(...values: Object) | Output a one CSV line with the values specified in values as each column |
| _CSVPrinter | lines(data: List<Object>, ...fieldNames: String) | Output CSV all lines from the array of records specified as data, with the values of the specified fieldNames as each column |


## Related example config files

* [BigQuery to Text](../../../../examples/bigquery-to-text.json)
* [FreeMarker template example](../../../../examples/bigquery-to-text.ftl)

