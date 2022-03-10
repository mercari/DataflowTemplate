# Copyfile Sink Module (Experimental)

Sink module for copying data using the value of record field

## Sink module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `copyfile` |
| input | required | String | Step name whose data you want to write from |
| parameters | required | Map<String,Object\> | Specify the following individual parameters. |

## Copyfile sink module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| sourceService | required | Enum | Specify one of `gcs`, `s3`, `drive` or `field` as the source storage service. If `field` is specified, the byte array in the specified field is uploaded to the destination. |
| destinationService | required | Enum | Specify one of `gcs`, `s3`, or `drive` as the destination storage service. |
| source | required | String | Specify the address of the file to be copied from. Can be assembled from record data using [Apache FreeMarker](https://freemarker.apache.org/). |
| destination | required | String | Specify the address of the file to be copied to. Can be assembled from record data using [Apache FreeMarker](https://freemarker.apache.org/). If destinationService is `drive`, specify the fileId that you want to be the parent folder of the file to be copied. |
| attributes | optional | Map<String,String\> | Specify the attribute name and value if you want to give attribute information such as `contentType` to the destination file. The value can be assembled from record data using [Apache FreeMarker](https://freemarker.apache.org/) |
| drive | conditionally required | DriveConfig | Specify additional Google Drive configuration if Google Drive is used in the source or destination storage service |
| s3 | conditionally required | S3Config | Specify additional AWS S3 configuration if S3 is used in the source or destination storage service |

## DriveConfig parameters

Google Drive read/write configuration

| parameter | optional | type | description |
| --- | --- | --- | --- |
| user | required | String | Specify service account to access Google Drive |

## S3Config parameters

AWS S3 read/write configuration

| parameter | optional | type | description |
| --- | --- | --- | --- |
| accessKey | required | String | Specify AWS accessKey to read/write s3. |
| secretKey | required | String | Specify AWS secretKey to read/write s3. |
| region | required | String | Specify AWS S3 region to read/write s3. |


## Related example config files

* [Copy Google Drive files to Cloud Storage](../../../../examples/drivefile-to-copyfile.json)
