# Mercari Dataflow Template

The Mercari Dataflow Template enable you to easily run various pipelines without writing a program, just by defining a simple configuration file.

See the [Document](docs/README.md) for usage

## Usage Example

Write the following json file and upload it to GCS (Suppose you upload it to gs://example/config.json).

This configuration file stores the BigQuery query results in the table specified by Spanner.

```json
{
  "sources": [
    {
      "name": "bigquery",
      "module": "bigquery",
      "parameters": {
        "query": "SELECT * FROM `myproject.mydataset.mytable`"
      }
    }
  ],
  "sinks": [
    {
      "name": "spanner",
      "module": "spanner",
      "input": "bigquery",
      "parameters": {
        "projectId": "myproject",
        "instanceId": "myinstance",
        "databaseId": "mydatabase",
        "table": "mytable"
      }
    }
  ]
}
```

Assuming you have deployed the Mercari Dataflow Template to gs://example/template, run the following command.

```sh
gcloud dataflow flex-template run bigquery-to-spanner \
  --template-file-gcs-location=gs://example/template \
  --parameters=config=gs://example/config.json
```

The Dataflow job will be started, and you can check the execution status of the job in the console screen.

<img src="https://raw.githubusercontent.com/mercari/DataflowTemplate/master/docs/images/bigquery-to-spanner.png">


## Deploy Template

### Requirements

* Java 8
* Maven 3

### Push Template Container Image to Cloud Container Registry.

```sh
mvn clean package -DskipTests -Dimage=gcr.io/{deploy_project}/{template_repo_name}
```

### Upload template file.

```sh
gcloud dataflow flex-template build gs://{path/to/template_file} \
  --image "gcr.io/{deploy_project}/{template_repo_name}" \
  --sdk-language "JAVA"
```

You can write legacy format template file manually and upload gcs.

```json
{
  "docker_template_spec": {
    "docker_image": "gcr.io/{deploy_project}/{template_repo_name}"
  }
}
```

## Run Template

* Run Dataflow Job from Template Container Image

gcloud command.
You can run template specifying gcs path that uploaded config file.

```sh
gsutil cp config.json gs://{path/to/config.json}

gcloud dataflow flex-template run {job_name} \
  --template-file-gcs-location=gs://{path/to/template_file} \
  --parameters=config=gs://{path/to/config.json}
```

You can also run template by REST API for using Legacy format template file.

```sh
PROJECT_ID=[PROJECT_ID]
REGION=us-central1
CONFIG="$(cat examples/xxxx.json)"
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


## Deploy Docker image for local pipeline

You can run pipeline locally. This is useful when you want to process small data quickly.

At first, you should register the container for local execution.


```sh
mvn clean package -DskipTests
docker build --tag=gcr.io/{deploy_project}/{repo_name_local} .
gcloud docker --project {deploy_project} -- push gcr.io/{deploy_project}/{repo_name_local}
```

## Run Pipeline locally

```sh
docker run \
  -v ~/.config/gcloud:/mnt/gcloud:ro \
  --rm gcr.io/{deploy_project}/{repo_name_local} \
  --project={project} \
  --config="$(cat config.json)"
```

## Committers

 * Yoichi Nagai ([@orfeon](https://github.com/orfeon))

## Contribution

Please read the CLA carefully before submitting your contribution to Mercari.
Under any circumstances, by submitting your contribution, you are deemed to accept and agree to be bound by the terms and conditions of the CLA.

https://www.mercari.com/cla/

## License

Copyright 2020 Mercari, Inc.

Licensed under the MIT License.