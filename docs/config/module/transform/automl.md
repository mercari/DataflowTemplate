# AutoML Transform Module (Experimental)

AutoML transform module sends prediction requests from the input data to a model created in AutoML, and gives the result to the output data.

## Transform module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `automl` |
| inputs | required | Array<String\> | Specify the names of the step from which you want to process the data, including the name of the transform. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters. |

## AutoML transform module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| endpoint | required | String | Specify the full resource name of the VertexAI endpoint to be used for prediction. (format: `projects/PROJECT/locations/LOCATION/endpoints/ENDPOINT_ID`) |
| type | required | String | Specify `table`, `vision`, `language`, or `video` as the prediction model type. (Currently, only `table` is supported). |
| objective | required | String | Specify one of `classification`, or `regression` as the target type of the prediction model. |
| prefix | optional | String | Specify if you want to give prefix to the additional field names of the prediction results. The default is empty string |
| batchSize | optional | Integer | Size of the data to send in a batch of requests to the endpoint. Sending a batch of data to some extent will improve throughput. The default is 32. |
| maxPeriodSecond | optional | Integer | Maximum time to wait for a request before the data reaches the `batchSize`. Only effective in streaming mode. The default is 10. |
| parallelNum | optional | Integer | Number of parallel requests sent to the Endpoint. The default is 1. |
| failFast | optional | Boolean | Specify whether to raise an exception when a request to the endpoint fails. if false, the failed data will be sent to `{name}.failures`. The default is true. |
| deployModel | optional | Boolean | Specify whether or not to deploy the specified model to the endpoint that has already been created but has not yet deployed the model before the job starts. The default is false. |
| undeployModel | optional | Boolean | Specify whether to deploy the model to the Endpoint before the job finishes. The default is false. |
| model | optional | DeployModel | Specify the contents of the model to be deployed when `deployModel` is true. |

### DeployModel parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| model | required | String | Specify the full resource name of the model to be deployed to the endpoint. (format: `projects/PROJECT/locations/LOCATION/models/MODEL_ID`) |
| machineType | optional | String | Specify the server [machine type](https://cloud.google.com/vertex-ai/docs/predictions/configure-compute) of the model to be deployed to the endpoint. The default is `n1-standard-2` |
| minReplicaCount | optional | Integer | Specifies the maximum number of servers for the model to be deployed to the endpoint. The default is 1. |
| maxReplicaCount | optional | Integer | Specifies the minimum number of servers for the model to be deployed to the endpoint. The default is the same as `minReplicaCount`. |


## Output schema additional fields 

The prediction result will contain the following fields in addition to the input data fields.
(The fields to be added depend on the `type` and `objective`)

[ref: https://cloud.google.com/vertex-ai/docs/predictions/interpreting-results-automl](https://cloud.google.com/vertex-ai/docs/predictions/interpreting-results-automl)


### regression

| field | type | description |
| --- | --- | --- |
| value | Float | Value of regression prediction |
| lower_bound | Float | Value of the lower 95% confidence interval. |
| upper_bound | Float | Value of the upper 95% confidence interval. |
| deployedModelId | String | ID of the deployed model that generated the prediction. |

### classification

| field | type | description |
| --- | --- | --- |
| scores | Array<Float\> | List of confidence scores how strongly the model associates each class with a test item. |
| classes | Array<String\> | List of classes to be classified. The order is the same as the order of the `scores`. |
| deployedModelId | String | ID of the deployed model that generated the prediction. |

## Related example config files

* [BigQuery AutoML predict to Cloud Spanner](../../../../examples/bigquery-to-automl-to-spanner.json)
