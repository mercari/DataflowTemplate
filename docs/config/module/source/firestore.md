# Cloud Firestore Source Module (Experimental)

Source Module for loading data from Cloud Firestore.

## Source module common parameters

| parameter          | optional | type                | description                                                                                                                   |
|--------------------|----------|---------------------|-------------------------------------------------------------------------------------------------------------------------------|
| name               | required | String              | Step name. specified to be unique in config file.                                                                             |
| module             | required | String              | Specify `firestore`                                                                                                           |
| schema             | required | [Schema](SCHEMA.md) | Schema of the data to be read.                                                                                                |
| timestampAttribute | optional | String              | If you want to use the value of an field as the event time, specify the name of the field. (The field must be Timestamp type) |
| parameters         | required | Map<String,Object\> | Specify the following individual parameters                                                                                   |

## Firestore source module parameters

| parameter      | optional | type           | description                                                                                                                             |
|----------------|----------|----------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| projectId      | optional | String         | GCP Project ID the Firestore you want to read. If not specified, it is automatically assigned dataflow executed environment project ID. |
| databaseId     | optional | String         | Database ID you want to load. The default is `(default)`.                                                                               |
| collection     | required | String         | Collection name you want to read.                                                                                                       |
| parent         | optional | String         | The parent if you want to load.                                                                                                         |
| filter         | optional | String         | Filter condition to read data from Firestore.                                                                                           |
| fields         | optional | Array<String\> | Fields names to filter the fields to be acquired.                                                                                       |
| allDescendants | optional | Boolean        | Selects only collections that are immediate children of the `parent` specified. The default is `false`.                                 |
| parallel       | optional | Boolean        | Specify `true` for parallel execution. If true, `allDescendants` must be set to `true`. The default is `false`.                         |

* If `parallel` is set to `true`, collection group index exemption for the fields to be used in the filter condition must be created.

## How to define filter condition

For filter condition, the comparison operators `=`, `>`, `<`, `>=`, `<=` and `!=` are available.
Only `AND` is available as a logical operator.

Below are examples of use.

```
timestampField >= '2022-01-01T15:00:00Z' AND timestampField < '2022-01-10T15:00:00Z'
```

(Appropriate indexes must be constructed to use filter conditions)


## Reserved fields in the schema

Special fields exist to retrieve document metadata.
Metadata can be fetched by defining the following fields in the [schema](SCHEMA.md)

| name             | type      | description                                      |
|------------------|-----------|--------------------------------------------------|
| `__name__`       | String    | Unique key name of the document                  |
| `__createtime__` | Timestamp | Timestamp the document was created               |
| `__updatetime__` | Timestamp | Timestamp the document was most recently updated |


## Related example config files

* [Cloud Firestore to BigQuery](../../../../examples/firestore-to-bigquery.json)
