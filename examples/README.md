# Example config files

Here is a list of configuration file examples for common data processing cases.

Try to find and arrange a configuration file that is similar to the data processing you want to do.

* Batch processing
  * Data Transfer
    * [BigQuery to Cloud Spanner](bigquery-to-spanner.json)
    * [BigQuery to Cloud Storage(Parquet)](bigquery-to-parquet.json)
    * [BigQuery to Cloud Datastore](bigquery-to-datastore.json)
    * [BigQuery to Cloud SQL](bigquery-to-jdbc.json)
    * [BigQuery to AWS S3(Avro)](bigquery-to-aws-avro.json)
    * [Cloud Spanner to BigQuery](spanner-to-bigquery.json)
    * [Cloud Spanner to Cloud Storage(Avro)](spanner-to-avro.json)
    * [Cloud Spanner to Cloud Datastore](spanner-to-datastore.json)
    * [Cloud Spanner to Cloud SQL](spanner-to-jdbc.json)
    * [Cloud Spanner to Cloud Spanner(Insert)](spanner-to-spanner.json)
    * [Cloud Spanner to Cloud Spanner(Delete)](spanner-to-spanner-delete.json)
    * [Cloud Storage(CSV) to Cloud Spanner](csv-to-spanner.json)
    * [Cloud Storage(Avro) to Cloud Spanner](avro-to-spanner.json)
    * [Cloud Storage(Avro) to Cloud Datastore](avro-to-datastore.json)
    * [Cloud Datastore to Cloud Storage(Avro)](datastore-to-avro.json)
    * [AWS S3(Avro) to Cloud Spanner](aws-avro-to-spanner.json)
    * [Cloud SQL to BigQuery](jdbc-to-bigquery.json)
    * [Cloud Storage(Spanner Backup) to Spanner](import-spanner-backup.json)
  * Data Processing
    * [BeamSQL: Join BigQuery and Spanner table](beamsql-join-bigquery-and-spanner-to-spanner.json)
    * [SetOperation: Replace Spanner Table](setoperation-replace-spanner.json)
* Streaming processing
  * Data Transfer
    * [Cloud PubSub(Avro) to BigQuery](pubsub-avro-to-bigquery.json)
    * [Cloud PubSub(Avro) to Cloud Spanner](pubsub-avro-to-spanner.json)
  * Microbatch
    * [Spanner(Microbatch) to BigQuery](spanner-microbatch-to-bigquery.json)
    * [BigQuery(Microbatch) to Spanner](bigquery-microbatch-to-spanner.json)