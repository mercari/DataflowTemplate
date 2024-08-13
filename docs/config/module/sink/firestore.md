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

| parameter     | optional | type           | description                                                                                                                                                                                                                                     |
|---------------|----------|----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| projectId     | optional | String         | GCP ProjectID with a Datastore to write to. If not specified, it is automatically assigned dataflow executed environment project ID.                                                                                                            |
| databaseId    | optional | String         | Database ID you want to write. The default is `(default)`                                                                                                                                                                                       |
| collection    | required | String         | Collection name to save. You can also specify a Collection name on the path using `nameTemplate`.                                                                                                                                               |
| nameFields    | optional | Array<String\> | Field names as unique keys when saving. Multiple fields may be specified. If both `nameFields` and `nameTemplate` are not specified, the name is automatically assigned by UUID. (No need to specify if the schema contains a `__name__` field) |
| nameTemplate  | optional | String         | Template text when you want to specify the name value by conversion using template engine [FreeMarker](https://freemarker.apache.org/). (No need to specify if the schema contains a `__name__` field)                                          |
| delete        | optional | Boolean        | True if you want to delete it instead of writing it. If true is specified, `nameFields` or `nameTemplate` must be specified. (No need to specify if the schema contains a `__name__` field)                                                     |
| rpcQos        | optional | PpcQosOptions  | Quality of Service manager options for Firestore RPCs.                                                                                                                                                                                          |

## PpcQosOptions parameter

| parameter              | optional | type     | description                                                                                                                 |
|------------------------|----------|----------|-----------------------------------------------------------------------------------------------------------------------------|
| batchInitialCount      | optional | Integer  | The initial size of a batch; used in the absence of the QoS system having significant data to determine a better batch size |
| batchMaxCount          | optional | Integer  | The maximum number of writes to include in a batch.                                                                         |
| batchTargetLatency     | optional | Integer  | Target latency for batch requests in second.                                                                                |
| initialBackoff         | optional | Integer  | The initial backoff duration to be used before retrying a request for the first time in seconds                             |
| maxAttempts            | optional | Integer  | The maximum number of times a request will be attempted for a complete successful result.                                   |
| overloadRatio          | optional | Integer  | The target ratio between requests sent and successful requests.                                                             |
| samplePeriod           | optional | Integer  | he length of time sampled request data will be retained in second.                                                          |
| samplePeriodBucketSize | optional | Integer  | The size of buckets within the specified `samplePeriod` in second.                                                          |
| throttleDuration       | optional | Integer  | The amount of time an attempt will be throttled if deemed necessary based on previous success rate in second.               |
| hintMaxNumWorkers      | optional | Integer  | A hint to the QoS system for the intended max number of workers for a pipeline.                                             |

### Assign Document ID

There are three ways to specify the ID of a document

* Specify the field with the value you want to use as the ID with `nameFields`.
* Specify a text template to generate the value you want to use as the ID with `nameTemplate`.
* Prepare a `__name__` field in the input record that will be used as the ID.

If the input record contains a field of `__name__`, it is used for the document ID and not for the property.

If nothing is specified, a UUID will be automatically generated as the ID.

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
