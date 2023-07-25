# Reshuffle Transform Module

Reshuffle transform module prevents fusion, checkpoints the data, and performs deduplication of records.
For more information on Dataflow fusion, please refer to the [Dataflow documentation](https://cloud.google.com/dataflow/docs/pipeline-lifecycle#preventing_fusion).


## Transform module common parameters

Reshuffle transform module requires no parameters.

| parameter  | optional | type                | description                                                    |
|------------|----------|---------------------|----------------------------------------------------------------|
| name       | required | String              | Step name. specified to be unique in config file.              |
| module     | required | String              | Specified `reshuffle`                                          |
| inputs     | required | Array<String\>      | Specify the names of the step from which you want to reshuffle |

## Related example config files

The following example deletes data from a specified table based on the results of a query to Spanner.
By placing reshuffle module between the spanner source, sink module in this case, the result of reading data from Spanner will be retained before proceeding to writing. This is expected to achieve greater throughput and finish the job in a shorter period of time.

* [Spanner to Spanner (Delete)](../../../../examples/spanner-to-spanner-delete.json)
