# Schema (Source module)

| name               | optional           | type          | description                                                                                               |
|--------------------|--------------------|---------------|-----------------------------------------------------------------------------------------------------------|
| fields             | selective required | Array<Field\> | Specify an array of type Field below.                                                                     |
| avroSchema         | selective required | String        | GCS path where you put the Avro schema file or directly specify the schmea in escaped JSON string format. |
| protobufDescriptor | selective required | String        | GCS path where you put the protobuf descriptor file.                                                      |

## Field

| name         | optional | type           | description                                                                                                                                                                                                                                                                 |
|--------------|----------|----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| name         | required | String         | Field name                                                                                                                                                                                                                                                                  |
| type         | required | Enum           | Field data type.                                                                                                                                                                                                                                                            |
| mode         | optional | Enum           | One of `nullable`, `required` and `repeated`. The default is `nullable`.                                                                                                                                                                                                    |
| fields       | optional | Array<Field\>  | If type is `record` then the field is specified as an array.                                                                                                                                                                                                                |
| defaultValue | optional | Primitive      | The default value to be inserted if the field does not exist or if the field value is null. If you specify for `array` type, it will be replaced if the value in the array is NULL. If for the `record` type, specify `"{}"` to substitute an empty record instead of NULL. |
| symbols      | optional | Array<String\> | Symbol array when you specify an `enum` type                                                                                                                                                                                                                                |

* For Array types, specify `repeated` in the `mode` field.
* If a field of mode `repeated` is NULL, an empty array will be substituted.

## Field data types

| type      | description                                                      |
|-----------|------------------------------------------------------------------|
| boolean   | `true` or `false`                                                |
| string    | text                                                             |
| json      | json text                                                        |
| bytes     | byte array                                                       |
| int       | integer 32bit                                                    |
| long      | integer 64bit                                                    |
| float     | float 32bit                                                      |
| double    | float 64bit                                                      |
| numeric   | numeric                                                          |
| date      | date                                                             |
| time      | time                                                             |
| timestamp | timestamp                                                        |
| enum      | enumeration. specify `symbols` when you specify this type        |
| record    | nested record field. specify `fields` when you specify this type |


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
              { "name": "childField2", "type": "long", "mode": "required", "defaultValue": 0 }
            ]
          }
        ]
      },
      "parameters": {
        "projectId": "myproject",
        "gql": "SELECT * FROM kind"
      }
    }
  ]
}
```