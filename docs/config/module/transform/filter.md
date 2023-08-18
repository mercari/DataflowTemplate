# Filter Transform Module

Filter transform module can be used to filter rows by specified criteria or to retrieve only specified columns.

This module provides two methods for field selection: `fields` and `select`.
Use `fields` (and `renameFields`) to refine or rename fields without modifying the fields.
If you want to do slight modification in addition to the refinement and renaming, use `select`.

## Transform module common parameters

| parameter  | optional | type                | description                                                                                                 |
|------------|----------|---------------------|-------------------------------------------------------------------------------------------------------------|
| name       | required | String              | Step name. specified to be unique in config file.                                                           |
| module     | required | String              | Specified `filter`                                                                                          |
| inputs     | required | Array<String\>      | Specify the names of the step from which you want to process the data, including the name of the transform. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters.                                                                |

## Filter transform module parameters

| parameter    | optional           | type                                  | description                                                                                                       |
|--------------|--------------------|---------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| filters      | selective required | [FilterCondition](filtercondition.md) | Specify the conditions for filtering rows.                                                                        |
| select       | selective required | Array<[SelectField](select.md)\>      | Specify a list of field names to be passed through. You can also specify nested fields by joining them with dots. |
| fields       | selective required | Array<String\>                        | Specify a list of field names to be passed through. You can also specify nested fields by joining them with dots. |
| renameFields | optional           | Map<String,String\>                   | To rename fields, specify the original name as the key and the name to be changed as the value.                   |

* It is not possible to not specify both `filters` and (`select` or `fields`)


## Related example config files

* [Split Avro records to Cloud Spanner](../../../../examples/avro-to-filter-to-avro.json)
* [BigQuery to Filter to Neo4jLocal](../../../../examples/bigquery-to-filter-to-localneo4j.json)