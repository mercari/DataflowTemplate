# Build-in modules

## Source Modules

| module                           | batch | streaming | description                                                        |
|----------------------------------|-------|-----------|--------------------------------------------------------------------|
| [bigquery](source/bigquery.md)   | ○     | ○         | Import data from BigQuery with a specified query or table          |
| [spanner](source/spanner.md)     | ○     | ○         | Import data from Cloud Spanner with a specified query or table     |
| [storage](source/storage.md)     | ○     | ○(TODO)   | Import data from file storage from specified path                  |
| [firestore](source/firestore.md) | ○     | ○(TODO)   | Import data from Cloud Firestore with a specified filter condition |
| [datastore](source/datastore.md) | ○     | ○(TODO)   | Import data from Cloud Datastore with a specified gql              |
| [jdbc](source/jdbc.md)           | ○     | ○(TODO)   | Import data from RDB using JDBC connector with a specified query   |
| [drivefile](source/drivefile.md) | ○     | -         | Import file info from Google Drive                                 |
| [pubsub](source/pubsub.md)       | -     | ○         | Import data from Cloud PubSub                                      |
| [websocket](source/websocket.md) | -     | ○         | Import data from WebSocket                                         |

## Transform Modules

| module | batch | streaming | description|
| --- | --- | --- | --- |
| [beamsql](transform/beamsql.md) | ○ | ○ | Process the data in a given SQL |
| [window](transform/window.md) | ○ | ○ | Assign to the specified type of Window from the time of the specified field |
| [union](transform/union.md) | ○ | ○ | Combines multiple inputs into a single input |
| [filter](transform/filter.md) | ○ | ○ | Filter rows by specified criteria or to retrieve only specified columns |
| [partition](transform/partition.md) | ○ | ○ | Splits a data collection into separate data collections based on specified conditions |
| [eventtime](transform/eventtime.md) | ○ | ○ | Extracts the event time from a record into a specified field, or sets the time value of a specified field as the event time of the record |
| [javascript](transform/javascript.md) | ○ | ○ | Processing input records with JavaScript functions. In the function, it is possible to refer to the values of past records and processed values as states. |
| [automl](transform/automl.md) | ○ | ○ | Send prediction requests to [Vertex AI endpoints](https://cloud.google.com/vertex-ai/docs/predictions/online-predictions-automl) and get the results. |
| [protobuf](transform/protobuf.md) | ○ | ○ | Deserialize a value serialized in ProtocolBuffer format. |
| [bandit](transform/bandit.md) | ○ | ○ | Outputs information about the arm to be selected by the [multi-arm bandit algorithm](https://en.wikipedia.org/wiki/Multi-armed_bandit). |
| [crypto](transform/crypto.md) | ○ | ○ | Encrypts or decrypts the value of a specified field.(Currently, only decryption is supported) |
| [tokenize](transform/tokenize.md) | ○ | ○ | Tokenizes and processes input text |
| [pdfextract](transform/pdfextract.md) | ○ | ○ | Extract text and metadata from PDF files |
| [bar](transform/bar.md) | ○ | ○ | Generate financial bars from transaction records |

## Sink Modules

| module | batch | streaming | description|
| --- | --- | --- | --- |
| [bigquery](sink/bigquery.md) | ○ | ○ | Inserting Data into BigQuery Table |
| [spanner](sink/spanner.md) | ○ | ○ | Inserting Data into Cloud Spanner Table |
| [storage](sink/storage.md) | ○ | ○(TODO) | Write file to Cloud Storage |
| [datastore](sink/datastore.md) | ○ | ○ | Inserting Data into Cloud Datastore kind |
| [bigtable](sink/bigtable.md) | ○ | ○ | Inserting Data into Cloud Bigtable table |
| [jdbc](sink/jdbc.md) | ○ | ○(TODO) | Inserting Data into RDB table using JDBC connector |
| [pubsub](sink/pubsub.md) | ○ | ○ | Publish data to specified PubSub topic |
| [text](sink/text.md) | ○ | ○(TODO) | Create text files with the template specified for each row |
| [copyfile](sink/copyfile.md) | ○ | ○(TODO) | Copy files between storage services |
| [solrindex](sink/solrindex.md) | ○ | - | Create solr index file to Cloud Storage |
| [debug](sink/debug.md) | ○ | ○ | Outputting data to the log |
