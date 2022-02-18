# Union Transform Module

Union transform module combines multiple inputs into a single input.
If the schemas of the inputs are different, they are merged into one schema.

## Transform module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `union` |
| inputs | required | Array<String\> | Specify the names of the step from which you want to process the data, including the name of the transform. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters. |

## Union transform module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| baseInput | optional | String | Specify the name of the input you want to use as the output schema. |
| mappings | optional | Array<Mapping\> | Specify the field name mapping to make inputs with different schemas conform to the baseInput schema. |

### Mapping parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| outputField | required | String | Specify the field name of baseInput |
| inputs | required | Array<MappingInput\> | Specify the field of the input that you want to map as the value of the outputField |

### MappingInput parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| input | required | String | Specify the name of the input you want to map |
| field | required | String | Specify the field name of the input you want to map |

## Schema mapping rule

* If both `baseInput` and `mappings` are not specified
  * The schema with all fields of the input will be output.
  * The fields of the input that do not have fields in the output schema will be null.
  * If there are fields with the same name but different types, an error will occur.
* If only `baseInput` is specified
  * The output will be the same schema as the input specified by `baseInput`.
  * Fields of input that have the same name and type as the input specified in `baseInput` will have their values output.
  * If there are fields with the same name but different types, an error will occur.
* If both `baseInput` and `mappings` are specified
  * The output will be the same schema as the input specified by `baseInput`.
  * You can define a mapping to the schema of the other input to match the schema of the input specified in `baseInput`.
  * If there are fields with the same name but different types, an error will occur.
* If only `mappings` is specified
  * Same as both `baseInput` and `mappings` are specified.
    

## Related example config files

* [Union Spanner records and store BigQuery](../../../../examples/pubsub-to-union-to-bigquery.json)
