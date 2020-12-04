# Execute Pipeline

## gcloud command

gcloud command allows you to execute a configuration file uploaded to GCS with parameters as follows

```sh
gsutil cp config.json gs://{path/to/config.json}

gcloud dataflow flex-template run {job_name} \
  --template-file-gcs-location=gs://{path/to/template_file} \
  --parameters=config=gs://{path/to/config.json}
```

## REST API

The REST API allows you to execute pipeline from configuration file on your PC.

(You should be aware that the commands are somewhat redundant and that you need to escape the JSON string in the configuration file.)

```sh
PROJECT_ID=[PROJECT_ID]
REGION=us-central1
CONFIG="$(cat config.json)"
curl -X POST -H "Content-Type: application/json"  -H "Authorization: Bearer $(gcloud auth print-access-token)" "https://dataflow.googleapis.com/v1b3/projects/${PROJECT_ID}/locations/${REGION}/templates:launch"`
  `"?dynamicTemplate.gcsPath=gs://{path/to/legacy_template_file}" -d "{
    'parameters': {
      'config': '$(echo "$CONFIG")'
    },
    'environment': {
        'tempLocation': 'gs://{path/to/tempfile}'
    },
    'jobName':'myJobName',
  }"
```

## Rewriting the configuration file at runtime

You can assign variables to the configuration file at runtime.

The first method is to use TemplateEngine as mentioned in the [How to Define Pipeline](../config/README.md).

The second way is to override the parameters in the Config file by specifying `{stepName}.{parameterName}` as a parameter at runtime.

Suppose you have defined a configuration file as follows.

```JSON
{
  "sources": [
    {
      "name": "myQueryResult",
      "module": "bigquery",
      "parameters": {
        "table": "xxx.yyy.zzz"
      }
    }
  ],
  ...
}
```

You can override the parameter by specifying a value with the parameter name DOM at runtime, as shown below.

```sh
gsutil cp config.json gs://{path/to/config.json}

gcloud dataflow flex-template run {job_name} \
  --template-file-gcs-location=gs://{path/to/template_file} \
  --parameters=config=gs://{path/to/config.json} \
  --parameters=myQueryResult.table=myproject.mydataset.mytable \
```