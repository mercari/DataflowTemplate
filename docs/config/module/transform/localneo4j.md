# LocalNeo4j Transform Module (Experimental)

LocalNeo4j transform module performs the specified Neo4j indexing and query with multiple inputs.

The same processing can be performed in batch and streaming.

## Transform module common parameters

| parameter  | optional | type                | description                                       |
|------------|----------|---------------------|---------------------------------------------------|
| name       | required | String              | Step name. specified to be unique in config file. |
| module     | required | String              | Specified `localNeo4j`                            |
| inputs     | required | Array<String\>      | Specify the input names to be aggregated.         |
| parameters | required | Map<String,Object\> | Specify the following individual parameters.      |

## LocalNeo4j transform module parameters

| parameter        | optional | type                | description                                                           |
|------------------|----------|---------------------|-----------------------------------------------------------------------|
| index            | required | IndexConfig         | Definition of input transformation from input data to graph database. |
| queries          | required | Array<QueryConfig\> | Definition of queries and responses to the graph database.            |
| groupFields      | optional | Array<String\>      | names of fields to be referenced to group the data.　                  |

## IndexConfig parameters

| parameter     | optional | type                                            | description                                                                                                                                                           |
|---------------|----------|-------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| path          | optional | String                                          | GCS path to initially load the graph database                                                                                                                         |
| database      | optional | String                                          | Database name. The default is `neo4j`                                                                                                                                 |
| nodes         | required | Array<[NodeConfig](../sink/localneo4j.md)\>     | Definition of conversion from input records to the [Node](https://neo4j.com/docs/getting-started/appendix/graphdb-concepts/#graphdb-node) to be saved                 |
| relationships | required | Array<[RelationConfig](../sink/localneo4j.md)\> | Definition of conversion from input records to the [Relationship](https://neo4j.com/docs/getting-started/appendix/graphdb-concepts/#graphdb-relationship) to be saved |
| setupCyphers  | optional | Array<String\>                                  | Cypher queries you wish to run at startup, such as index definitions.                                                                                                 |
| bufferSize    | optional | Integer                                         | Buffer size to write to the database. The default is 500                                                                                                              |

## QueryConfig parameters

| parameter | optional | type                          | description                                                                                                                                                                                                   |
|-----------|----------|-------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| name      | required | String                        | Name of the output from this query. Refer to the latter module as `{moduleName}.{queryName}`.                                                                                                                 |
| input     | required | String                        | Input name to be used for query generation                                                                                                                                                                    |
| fields    | optional | Array<String\>                | Names of fields in the input record that you want to use as output data.                                                                                                                                      |
| cypher    | required | String                        | Specify the Cypher text to be used for the query generation in the Template Engine, [Apache FreeMarker](https://freemarker.apache.org/) format. The template can reference fields values in the input record. |
| schema    | required | [Schema](../source/SCHEMA.md) | Specify the schema of the Cypher query results. The data of the schema specified here will be stored as an array in the field `results` as the result of the query.                                           |

## Related example config files

* [BigQuery to LocalNeo4j to BigQuery](../../../../examples/bigquery-to-localneo4j-to-bigquery.json)
