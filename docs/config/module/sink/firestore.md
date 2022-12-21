# Firestore Sink Module (Experimental)

Sink module to write the input data to a specified Cloud Firestore collection.

## Sink module common parameters

| parameter  | optional | type                | description                                       |
|------------|----------|---------------------|---------------------------------------------------|
| name       | required | String              | Step name. specified to be unique in config file. |
| module     | required | String              | Specified `firestore`                             |
| input      | required | String              | Step name whose data you want to write from       |
| parameters | required | Map<String,Object\> | Specify the following individual parameters.      |

## Firestore sink module parameters

| parameter     | optional | type           | description                                                                                                                                                                                 |
|---------------|----------|----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| projectId     | optional | String         | GCP ProjectID with a Datastore to write to. If not specified, it is automatically assigned dataflow executed environment project ID.                                                        |
| databaseId    | optional | String         | Database ID you want to write. The default is `(default)`                                                                                                                                   |
| collection    | required | String         | Collection name to save. You can also specify a Collection name on the path using `nameTemplate`.                                                                                           |
| nameFields    | optional | Array<String\> | Field names as unique keys when saving. Multiple fields may be specified. If both `nameFields` and `nameTemplate` are not specified, the name is automatically assigned by UUID.            |
| nameTemplate  | optional | String         | Template text when you want to specify the name value by conversion using template engine [FreeMarker](https://freemarker.apache.org/)                                                      |
| delete        | optional | Boolean        | True if you want to delete it instead of writing it. If true is specified, `nameFields` or `nameTemplate` must be specified. (No need to specify if the schema contains a `__name__` field) |


### Signatures of build-in utility functions for template engine

There are built-in functions for date and timestamp formatting available in the `nameTemplate`.

```
// Text format function for date field
${_DateTimeUtil.formatDate(dateField, 'yyyyMMdd')}

// Text format function for timestamp field
${_DateTimeUtil.formatTimestamp(timestampField, 'yyyyMMddhhmmss', 'Asia/Tokyo')}

// The event timestamp implicitly assigned to a record can be referenced by _EVENTTIME.
${_DateTimeUtil.formatTimestamp(_EVENTTIME, 'yyyyMMddhhmmss')}
```

## Related example config files

* [BigQuery to Cloud Firestore](../../../../examples/bigquery-to-firestore.json)
