# Vertex AI Matching Engine Sink Module (Experimental)

Sink module to insert vectors to a specified Matching Engine index.

## Sink module common parameters

| parameter  | optional | type                | description                                       |
|------------|----------|---------------------|---------------------------------------------------|
| name       | required | String              | Step name. specified to be unique in config file. |
| module     | required | String              | Specified `matchingEngine`                        |
| input      | required | String              | Step name whose data you want to write from       |
| parameters | required | Map<String,Object\> | Specify the following individual parameters.      |

## Matching Engine sink module parameters

| parameter                     | optional | type                | description                                                                                                                                                                                                   |
|-------------------------------|----------|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| projectId                     | optional | String              | GCP ProjectID with a Matching Engine to write to. If not specified, it is automatically assigned dataflow executed environment project ID.                                                                    |
| region                        | required | String              | Region in which matching engine index is created                                                                                                                                                              |
| indexId                       | required | String              | Index ID of the matching engine to which data is to be submitted                                                                                                                                              |
| mode                          | optional | Enum                | Feed mode, specifying `upsert` or `remove`. The default is `upsert`.                                                                                                                                          |
| idField                       | optional | String              | Specify the name of the field with the value you want as the ID of the datapoint. If not specified, the id is automatically assigned by UUID. Required if mode is `remove`.                                   |
| vectorField                   | optional | String              | Specify the name of the field whose value you want to use as a vector of datapoint. Required if mode is `upsert`.                                                                                             |
| crowdingTagField              | optional | String              | Specify the name of the field whose value you want to use as a [crowdingTag](https://cloud.google.com/vertex-ai/docs/reference/rest/v1/projects.locations.indexes/upsertDatapoints#CrowdingTag) of datapoint. |
| restricts                     | optional | Array<Restriction\> | Specify [restrictions](https://cloud.google.com/vertex-ai/docs/reference/rest/v1/projects.locations.indexes/upsertDatapoints#restriction) conditions for data retrieval.                                      |
| bufferSize                    | optional | Integer             | Size to group requests to the Index. The default is 100                                                                                                                                                       |
| maxBufferFlushIntervalSeconds | optional | Integer             | Maximum wait seconds for flush buffer to index when running in streaming mode. The default is 10.                                                                                                             |


## Restriction parameter

| parameter  | optional | type   | description                                                                 |
|------------|----------|--------|-----------------------------------------------------------------------------|
| namespace  | required | String | The namespace of this restriction. eg: color.                               |
| allowField | optional | String | The name of field value you want to use as allowed value in this namespace. |
| denyField  | optional | String | The name of field value you want to use as denied value in this namespace.  |

## Retrieving records that failed to insert

If an unintended error occurs during data insertion, the failure records can be retrieved from `{name}.failures` as the output of this module.
Insertion is submitted in batches, so if a failure occurs, the all records in the batch will go through.

## Related example config files

* [BigQuery to MatchingEngine](../../../../examples/bigquery-to-matchingengine.json)
* [PubSub to MatchingEngine](../../../../examples/pubsub-to-matchingengine.json)
