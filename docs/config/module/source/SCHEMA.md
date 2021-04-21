# Schema (Source module)

| name | optional | type | description |
| --- | --- | --- | --- |
| fields |  selective required | Array<Field\> | Specify an array of type Field below. |
| avroSchema | selective required | String | GCS path where you put the Avro schema file. |
| protobufDescriptor | selective required | String | GCS path where you put the protobuf descriptor file. |

## Field

| name | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Field name |
| type | required | Enum | Field data type. |
| mode | optional | Enum | One of `nullable`, `required` and `repeated`. The default is `nullable`. |
| fields | optional | Array<Field\> | If type is `record` then the field is specified as an array. |

※ For array types, specify `repeated` in the `mode` field.

## Field data types

| type | description |
| --- | --- |
| string | text |
| boolean | `true` or `false` |
| integer | int 32bit |
| long | int 64bit |
| float | float 32bit |
| double | float 64bit |
| bytes | byte array |
| date | date |
| timestamp | datetime |
| record | nested record field |


## Example

An example of specifying a schema when loading data from Cloud Datastore.

```json
{
  "sources": [
    {
      "name": "datastore",
      "module": "datastore",
      "schema": {
        "fields": [
          { "name": "field1", "type": "string", "mode": "nullable" },
          { "name": "field2", "type": "integer", "mode": "nullable" },
          { "name": "field3", "type": "bytes", "mode": "required" },
          { "name": "field4", "type": "string", "mode": "repeated" },
          {
            "name": "nestedField",
            "type": "record",
            "mode": "nullable",
            "fields": [
              { "name": "childField1", "type": "string", "mode": "nullable" },
              { "name": "childField2", "type": "integer", "mode": "nullable" }
            ]
          },
        ]
      },
      "parameters": {
        "projectId": "myproject",
        "gql": "SELECT * FROM kind"
      }
    },
  ]
}
```