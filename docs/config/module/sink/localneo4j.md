# LocalNeo4j Sink Module (Experimental)

Sink module to create Neo4j database file to specified GCS path.

The index files generated by this module can be easily imported into [Neo4j docker official image](https://hub.docker.com/_/neo4j/) to build Neo4j API server.
(This module generates database file for Neo4j version 4)

## Sink module common parameters

| parameter  | optional | type                | description                                       |
|------------|----------|---------------------|---------------------------------------------------|
| name       | required | String              | Step name. specified to be unique in config file. |
| module     | required | String              | Specified `localNeo4j`                            |
| inputs     | required | Array<String\>      | Step names whose data you want to write from      |
| parameters | required | Map<String,Object\> | Specify the following individual parameters.      |

## LocalNeo4j sink module parameters

| parameter     | optional | type                   | description                                                                                                                                                           |
|---------------|----------|------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| output        | required | String                 | GCS path for neo4j database file writing destination. Database files are compressed as a zip file.                                                                    |
| input         | optional | String                 | GCS path for neo4j database file writing destination. Index files are compressed as a zip file.                                                                       |
| database      | optional | String                 | Database name. The default is `neo4j`                                                                                                                                 |
| nodes         | required | Array<NodeConfig\>     | Definition of conversion from input records to the [Node](https://neo4j.com/docs/getting-started/appendix/graphdb-concepts/#graphdb-node) to be saved                 |
| relationships | required | Array<RelationConfig\> | Definition of conversion from input records to the [Relationship](https://neo4j.com/docs/getting-started/appendix/graphdb-concepts/#graphdb-relationship) to be saved |
| setupCyphers  | optional | Array<String\>         | Specify the Cypher queries you wish to run at startup, such as index definitions.                                                                                     |
| bufferSize    | optional | Integer                | Specifies the buffer size to write to the database. The default is 500                                                                                                |
| tempDirectory | optional | String                 | The GCS path of the temporary file export destination. If not specified, the bucket creation permission is required.                                                  |

## NodeConfig parameters

| parameter      | optional | type           | description                                                                                                                                                                                 |
|----------------|----------|----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| input          | required | String         | Specify the input name used for node generation.                                                                                                                                            |
| labels         | required | Array<String\> | Specify labels to be assigned to the generated node.                                                                                                                                        |
| keyFields      | required | Array<String\> | Specify fields with unique key values to be assigned to the node to be generated. Input record schema must have these fields. The values of fields specified here are stored as properties. |
| propertyFields | optional | Array<String\> | Specify fields you wish to assign as properties to the node to be generated. Input record schema must have these fields.                                                                    |

## RelationConfig parameters

| parameter      | optional | type           | description                                                                                                                                                                                 |
|----------------|----------|----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| input          | required | String         | Specify the input name used for relationship generation.                                                                                                                                    |
| type           | required | String         | Specify the type name to be assigned to the generated relationship.                                                                                                                         |
| source         | required | RelNodeConfig  | Specifies source node information for the relationship.                                                                                                                                     |
| target         | required | RelNodeConfig  | Specifies target node information for the relationship.                                                                                                                                     |
| keyFields      | required | Array<String\> | Specify fields with unique key values to be assigned to the node to be generated. Input record schema must have these fields. The values of fields specified here are stored as properties. |
| propertyFields | optional | Array<String\> | Specify fields you wish to assign as properties to the node to be generated. Input record schema must have these fields.                                                                    |

## RelNodeConfig parameters

| parameter      | optional | type           | description                                                                 |
|----------------|----------|----------------|-----------------------------------------------------------------------------|
| label          | required | String         | Specify a label to filter node.                                             |
| keyFields      | required | Array<String\> | Specify fields with unique key values to get node.                          |
| propertyFields | optional | Array<String\> | Specify properties to be given if the node you tried to get does not exist. |

## Import database file into Neo4j official docker image

To import the generated index file into the official Neo4j docker image, download the database file, unzip, and run the Dockerfile as shown in the example below.

* [Dockerfile](../../../../examples/Dockerfile_graph)

To upload an image with the database file included to GCR using this Dockerfile, execute the following command

```bash
docker build -t ${REGION}-docker.pkg.dev/${PROJECT_ID}/graph/graph .
```

## Deploy Neo4j API server on Cloud Run

To deploy a Neo4j API server on Cloud Run using Cloud Build, prepare and run the following cloudbuild file.

* [cloudbuild.yaml](../../../../examples/cloudbuild_graph.yaml)

Then execute the following command.

```shell
gcloud builds submit --config=cloudbuild.yaml --project=myproject
```

## Run local

To download and unzip the database file locally and mount it for use when launching the Neo4j image, execute the following command

```shell
docker run \
    --name graph \
    -p7474:7474 -p7687:7687 \
    -d \
    -v {graph_db_dir_path}/data:/data \
    -v {graph_db_dir_path}/logs:/logs \
    -v {graph_db_dir_path}/import:/var/lib/neo4j/import \
    --env NEO4J_AUTH=neo4j/password \
    neo4j:4.4.21
```

## Related example config files

* [BigQuery to LocalNeo4j](../../../../examples/bigquery-to-localneo4j.json)