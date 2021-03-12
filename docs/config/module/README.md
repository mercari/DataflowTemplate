# Build-in modules

## Source Modules

※ In streaming mode for modules other than PubSub will run in micro batches

| module | batch | streaming | description |
| --- | --- | --- | --- |
| [bigquery](source/bigquery.md) | ○ | ○ | Import data from BigQuery with a specified query or table |
| [spanner](source/spanner.md) | ○ | ○ | Import data from Cloud Spanner with a specified query or table |
| [storage](source/storage.md) | ○ | ○(TBA) | Import data from file storage from specified path |
| [datastore](source/datastore.md) | ○ | ○ (TBA) | Import data from Cloud Datastore with a specified gql |
| [jdbc](source/jdbc.md) | ○ | ○(TBA) | Import data from RDB using JDBC connector with a specified query |
| [pubsub](source/pubsub.md) | - | ○ | Import data from Cloud PubSub |

## Transform Modules

| module | batch | streaming | description|
| --- | --- | --- | --- |
| [beamsql](transform/beamsql.md) | ○ | ○ | Process the data in a given SQL |
| [window](transform/window.md) | ○ | ○ | Assign to the specified type of Window from the time of the specified field |
| [partition](transform/partition.md) | ○ | ○ | Splits a data collection into separate data collections based on specified conditions |
| [protobuf](transform/protobuf.md) | ○ | ○ | Deserialize a value serialized in ProtocolBuffer format. |
| [crypto](transform/crypto.md) | ○ | ○ | Encrypts or decrypts the value of a specified field.(Currently, only decryption is supported) |
| [pdfextract](transform/pdfextract.md) | ○ | ○ | Extract text and metadata from PDF files |

## Sink Modules

| module | batch | streaming | description|
| --- | --- | --- | --- |
| [bigquery](sink/bigquery.md) | ○ | ○ | Inserting Data into BigQuery Table |
| [spanner](sink/spanner.md) | ○ | ○ | Inserting Data into Cloud Spanner Table |
| [storage](sink/storage.md) | ○ | ○(TBA) | Write file to Cloud Storage |
| [datastore](sink/datastore.md) | ○ | ○ | Inserting Data into Cloud Datastore Table |
| [jdbc](sink/jdbc.md) | ○ | ○(TBA) | Inserting Data into RDB table using JDBC connector |
| [pubsub](sink/pubsub.md) | ○ | ○ | Publish data to specified PubSub topic |
| [text](sink/text.md) | ○ | ○(TBA) | Create text files with the template specified for each row |
