# Datastore Sink Module

Sink module to write the input data to a specified Cloud Datastore kind.

## Sink module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `datastore` |
| input | required | String | Step name whose data you want to write from |
| parameters | required | Map<String,Object\> | Specify the following individual parameters. |

## Datastore sink module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| projectId | required | String | GCP ProjectID with a Datastore to write to |
| kind | required | String | Kind name to save |
| keyFields | optional | Array<String\> | Field names as unique keys when saving. Multiple fields may be specified. If not specified, it is automatically assigned by UUID. |
| keyTemplate | optional | String | Specify the template text when you want to specify the key value by conversion using template engine [FreeMarker](https://freemarker.apache.org/) |
| delete | optional | Boolean | true if you want to delete it instead of writing it. If true is specified, `keyFields` or `keyTemplate` must be specified. (No need to specify if the schema contains a `__key__` field) |

### Signatures of build-in utility functions for template engine

There are built-in functions for date and timestamp formatting available in the `keyTemplate`.

```
// Text format function for date field
${_DateTimeUtil.formatDate(dateField, 'yyyyMMdd')}

// Text format function for timestamp field
${_DateTimeUtil.formatTimestamp(timestampField, 'yyyyMMddhhmmss', 'Asia/Tokyo')}

// The event timestamp implicitly assigned to a record can be referenced by _EVENTTIME.
${_DateTimeUtil.formatTimestamp(_EVENTTIME, 'yyyyMMddhhmmss')}
```

## Related example config files

* [BigQuery to Cloud Datastore](../../../../examples/bigquery-to-datastore.json)
* [Cloud Spanner to Cloud Datastore](../../../../examples/spanner-to-datastore.json)
* [Cloud Storage(Avro) to Cloud Datastore](../../../../examples/avro-to-datastore.json)
* [Cloud Datestore to Delete](../../../../examples/datastore-to-delete.json)
