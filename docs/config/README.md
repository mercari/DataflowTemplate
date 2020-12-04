# Define Pipeline

## Config file contents

In the Config file, four modules, `settings`, `sources`, `transforms`, and `sinks`, are combined to define the processing contents.
`sources` is for input data acquisition, `transforms` is for data processing, and `sinks` is for data output.
`settings` defines common settings for sources, transforms, and sinks.

```JSON:config
{
  "settings": [
    {...}
  ],
  "sources": [
    {...}
  ],
  "transforms": [
    {...}
  ],
  "sinks": [
    {...}
  ]
}
```

You can define and run a pipeline by combining these three types of various build-in modules.

The list of build-in modules can be found on [Modules Page](module/README.md).

Examples of configuration files are listed in the [Examples Page](../../examples/README.md), so try to find and arrange a configuration file that is close to the data processing you want to perform.

Below is an overview of these built-in modules.

## Module common parameter

In the three types of modules, the contents of input, processing, and output are described as JSON parameters.
The common settings of the three types of modules are as follows.

| parameter | type | optional | description |
| --- | --- | --- | --- |
| name | String | required | Set unique name in Config JSON |
| module | String | required | Set [module](module/README.md) name |
| parameters | Map<String, Object\> | required | Specify the parameters defined in each module. |


## Source modules

The source is a module that defines the source of the data you want to process in the pipeline.
Common configuration items in the source module are as follows.

| parameter | type | optional | description |
| --- | --- | --- | --- |
| schema | [Schema](module/source/SCHEMA.md) | optional | Specifies the schema of the input resource. If the input resource has schema information, no specification is required. |
| timestampAttribute | String | optional | Defines which fields of the source record should be treated as EventTime. The default is the time of input. |


## Transform modules

The transform is a module that defines what to do with the data.
The common settings of the transform module are as follows.

| parameter | type | optional | description |
| --- | --- | --- | --- |
| inputs | Array<String\> | required | Specify the names of the module from which you want to process the data, including the name of the transform. |


## Sink modules

The sink is a module that defines the output destination of the data.
The common settings of the sink module are as follows

| parameter | type | optional | description |
| --- | --- | --- | --- |
| input | String | required | Specify the name of the module from which you want to output data. source or transform name. |
| wait | Array<String\> | optional | If you want to wait for the completion of other steps and then start the output, assign a step Name to wait for completion. |

## Settings

settings is a module that defines common settings for all modules.
The following items can be defined as settings.

| parameter | type | description |
| --- | --- | --- |
| aws.accessKey | String | Set aws accessKey to dataflow worker use. If read/write s3, required. |
| aws.secretKey | String | Set aws secretKey to dataflow worker use. If read/write s3, required. |
| aws.region | String | Set aws s3 region. If read/write s3, required. |
| beamsql.plannerName | String | Choose BeamSQL planner class name. `org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner` or `org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner`. default is zetasql. |


```JSON:settings
"settings": {
    "aws": {
        "accessKey": "{AWS_ACCESSKEY}",
        "secretKey": "{AWS_SECRETKEY}",
        "region": "{AWS_REGION}"
    },
    "beamsql": {
        "plannerName": "{beam planner class name}"
    }
}
```

## Rewriting the configuration file at runtime

In the configuration file, you can use the Template Engine, [Apache FreeMarker](https://freemarker.apache.org/), to assign variables at runtime, or you can even rewrite the file itself.

You can define variables in the configuration file, as in the example below, and assign values at run time.
The notation follows the FreeMarker specification.

```JSON
{
  "sources": [
    {
      "name": "MyKindInput",
      "module": "datastore",
      "timestampAttribute": "created_at",
      "schema": {
        "fields": []
      },
      "parameters": {
        "projectId": "myproject",
        "gql": "SELECT * FROM MyKind WHERE created_at > DATETIME('${current_datetime}')"
      }
    }
  ],
  "sinks": [
    {
      "name": "MyKindOutput",
      "module": "storage",
      "input": "MyKindInput",
      "parameters": {
        "output": "${output_path}",
        "format": "avro"
      }
    }
  ]
}
```

You can assign variables to the Config file at runtime by prefixing it with the parameter `template.`.

```sh
gcloud dataflow flex-template run {job_name} \
  --template-file-gcs-location=gs://{path/to/template_file} \
  --parameters=config=gs://{path/to/config.json} \
  --parameters=template.current_datetime=2020-12-01T00:00:00Z \
  --parameters=template.output_path=gs://mybucket/output
```

REST API version

```sh
CONFIG="$(cat examples/xxxx.json)"
curl -X POST -H "Content-Type: application/json"  -H "Authorization: Bearer $(gcloud auth print-access-token)" "https://dataflow.googleapis.com/v1b3/projects/${PROJECT_ID}/locations/${REGION}/templates:launch"`
  `"?dynamicTemplate.gcsPath=gs://{path/to/legacy_template_file}" -d "{
    'parameters': {
      'config': '$(echo "$CONFIG")',
      'template.current_datetime': '2020-12-01T00:00:00Z',
      'template.output_path': 'gs://mybucket/output',
    },
    'jobName':'myJobName',
  }"
```