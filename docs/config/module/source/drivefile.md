# Google Drive File Source Module

Source Module for loading file information by specifying a query into Google Drive.

## Source module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `drivefile` |
| schema | - | [Schema](SCHEMA.md) | Schema of the data to be read. drivefile module does not require specification. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters |

## Drivefile source module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| query | required | String | Specify a [query](https://developers.google.com/drive/api/v3/search-files#query_string_examples) for Google Drive. |
| user | required | String | Specify service account to access Google Drive |
| folderId | optional | String | Specify the folder ID if you want to search under the folder. |
| driveId | optional | String | Specify ID of the shared drive to search. |
| fields | optional | String | Specify the [fields](https://developers.google.com/drive/api/v3/reference/files) you want to retrieve. Nesting field is not supported, default is as follows |
| recursive | optional | Boolean | Specify if you want to get files under the folder recursively. Default is true. |

* Default fields: `files(id,driveId,name,size,description,version,originalFilename,kind,mimeType,fileExtension,parents,createdTime,modifiedTime)`

## Preparation

### Enable API

* [Google Drive API](https://console.developers.google.com/apis/library/drive.googleapis.com)
* [IAM Service Account Credentials API](https://console.developers.google.com/apis/library/iamcredentials.googleapis.com)

### Assign role

The drivefile module uses [impersonating service accounts](https://cloud.google.com/iam/docs/impersonating-service-accounts).

The following roles must be assigned to dataflow worker service accounts to impersonate a drive user service account

* Service Account User (roles/iam.serviceAccountUser)
* Service Account Token Creator (roles/iam.serviceAccountTokenCreator)

In addition to that, the service account specified in `user` must also be granted read permission to the Drive to be retrieved by the `query`.

## Related example config files

* [Copy Google Drive files to Cloud Storage](../../../../examples/drivefile-to-copyfile.json)
