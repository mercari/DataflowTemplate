# JDBC Sink Module

Sink module to insert, update, delete input records to a specified RDB table.

## Sink module common parameters

| parameter  | optional  | type                | description                                       |
|------------|-----------|---------------------|---------------------------------------------------|
| name       | required  | String              | Step name. specified to be unique in config file. |
| module     | required  | String              | Specified `jdbc`                                  |
| inputs     | required  | Array<String\>      | Step name whose data you want to write from       |
| parameters | required  | Map<String,Object\> | Specify the following individual parameters.      |

## JDBC sink module parameters

| parameter      | optional | type           | description                                                                                                                                            |
|----------------|----------|----------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| table          | required | String         | Destination table name.                                                                                                                                |
| url            | required | String         | Connection destination for reading data in JDBC.                                                                                                       |
| driver         | required | String         | Specify driver class such as `com.mysql.cj.jdbc.Driver`, `org.postgresql.Driver`                                                                       |
| user           | required | String         | User name to access the database. You can also specify a Secret Manager resource name like `projects/{myproj}/secrets/{mysecret}/versions/latest`.     |
| password       | required | String         | User password to access the database. You can also specify a Secret Manager resource name like `projects/{myproj}/secrets/{mysecret}/versions/latest`. |
| op             | optional | String         | Write operation. `INSERT` supports all databases, and `INSERT_OR_UPDATE` and `INSERT_OR_DONOTHING` support MySQL and PostgreSQL. The default is `INSERT`. |
| batchSize      | optional | Integer        | Number of bulk operations to process together before committing. The default is 1000.                                                                  |
| bulkInsertSize | optional | Integer        | Maximum number of records to write in a single bulk operation. The default is 1. SQL Server supports a maximum of 1000.                                |
| createTable    | optional | Boolean        | Specify true if you want to generate the table automatically if the destination table does not exist.                                                  |
| emptyTable     | optional | Boolean        | Specify true if you want to delete all data from the destination table before inserting data.                                                          |
| keyFields      | optional | Array<String\> | Primary key fields used to identify matching records. Required for `INSERT_OR_UPDATE` and `INSERT_OR_DONOTHING`.                                       |

`bulkInsertSize` controls the maximum number of records written in one bulk operation, while `batchSize` controls the number of bulk operations processed before committing. For example, with `bulkInsertSize: 100` and `batchSize: 10`, up to 100 records are written per bulk operation and up to 1,000 records are processed before each commit. The final operation may contain fewer records.

* url examples
  * MySQL for Cloud SQL
    * `jdbc:mysql://google/mydatabase?cloudSqlInstance=myproject:us-central1:myinstance&socketFactory=com.google.cloud.sql.mysql.SocketFactory`
  * PostgreSQL for Cloud SQL
    * `jdbc:postgresql://google/mydatabase?cloudSqlInstance=myproject:us-central1:myinstance&socketFactory=com.google.cloud.sql.postgres.SocketFactory`
  * PostgreSQL for AlloyDB
    * `jdbc:postgresql:///mydatabase?alloydbInstanceName=projects/myproject/locations/us-central1/clusters/mycluster/instances/myinstance-primary&socketFactory=com.google.cloud.alloydb.SocketFactory`

## Related example config files

* [BigQuery to Cloud SQL](../../../../examples/bigquery-to-jdbc.json)
* [BigQuery to AlloyDB](../../../../examples/bigquery-to-alloydb.yaml)
* [Cloud Spanner to Cloud SQL](../../../../examples/spanner-to-jdbc.json)
